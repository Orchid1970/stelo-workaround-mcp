#!/usr/bin/env python3
"""
FastAPI app to import Dexcom Stelo/Clarity CSVs into SQLite with auto-migrations.
Run with: uvicorn main:app --host 0.0.0.0 --port 8085
"""

import os
import csv
import sqlite3
import io
import math
from datetime import datetime, timezone
from typing import Optional, List, Tuple, Dict, Any

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import JSONResponse
from dateutil import parser as dtparser

DB_PATH = os.environ.get("DB_PATH", os.path.expanduser("~/data/glucose.db"))
DB_DIR = os.path.dirname(DB_PATH)
LATEST_SCHEMA_VERSION = 2

app = FastAPI(title="Dexcom Stelo/Clarity -> SQLite importer", version="1.0")
os.makedirs(DB_DIR, exist_ok=True)


# --- SQLite & Migrations ---
def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn


def _apply_migration_v1(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS glucose (
        timestamp_unix INTEGER PRIMARY KEY,
        glucose_mg_dl INTEGER NOT NULL,
        source TEXT DEFAULT 'clarity',
        created_at TEXT DEFAULT (datetime('now'))
    );""")
    cur.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1');")
    conn.commit()


def _apply_migration_v2(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(glucose);")
    cols = [r["name"] for r in cur.fetchall()]
    if "notes" not in cols:
        cur.execute("ALTER TABLE glucose ADD COLUMN notes TEXT;")
    cur.execute("UPDATE meta SET value='2' WHERE key='schema_version';")
    conn.commit()


MIGRATIONS = {1: _apply_migration_v1, 2: _apply_migration_v2}


def ensure_migrations():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='meta';")
        if cur.fetchone() is None:
            _apply_migration_v1(conn)
        cur.execute("SELECT value FROM meta WHERE key='schema_version';")
        row = cur.fetchone()
        current = int(row["value"]) if row else 0
        for v in range(current + 1, LATEST_SCHEMA_VERSION + 1):
            if fn := MIGRATIONS.get(v):
                fn(conn)
    finally:
        conn.close()


ensure_migrations()


# --- Parsing ---
def parse_timestamp_to_unix(ts_str: str) -> int:
    dt = dtparser.parse(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp())


def detect_columns(header_row):
    low = [h.strip().lower() for h in header_row]
    ts_idx = gl_idx = None
    for i, h in enumerate(low):
        if any(x in h for x in ["timestamp", "date", "time", "datetime"]):
            ts_idx = i
        if any(x in h for x in ["glucose", "mg/dl", "value", "sgv"]):
            gl_idx = i
    if ts_idx is None or gl_idx is None:
        raise ValueError(f"Could not detect columns from: {header_row}")
    return ts_idx, gl_idx


def parse_clarity_csv_content(content: bytes):
    text = content.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return []

    header_row = None
    first_data_idx = 1
    for i, r in enumerate(rows):
        if r and any(cell.strip() for cell in r):
            header_row = r
            first_data_idx = i + 1
            break
    if not header_row:
        return []

    ts_idx, gl_idx = detect_columns(header_row)
    parsed = {}
    for r in rows[first_data_idx:]:
        if not r or len(r) <= max(ts_idx, gl_idx):
            continue
        try:
            ts_raw, gl_raw = r[ts_idx].strip(), r[gl_idx].strip()
            if ts_raw and gl_raw:
                parsed[parse_timestamp_to_unix(ts_raw)] = int(float(gl_raw))
        except:
            continue
    return sorted(parsed.items())


def insert_glucose_rows(rows, source="clarity"):
    if not rows:
        return {"total": 0, "inserted": 0, "skipped_existing": 0}
    conn = get_conn()
    total_before = conn.execute("SELECT count(*) FROM glucose").fetchone()[0]
    now_str = datetime.utcnow().isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO glucose(timestamp_unix, glucose_mg_dl, source, created_at) VALUES (?, ?, ?, ?)",
        [(ts, gl, source, now_str) for ts, gl in rows]
    )
    conn.commit()
    total_after = conn.execute("SELECT count(*) FROM glucose").fetchone()[0]
    conn.close()
    inserted = total_after - total_before
    return {"total": len(rows), "inserted": inserted, "skipped_existing": len(rows) - inserted}


# --- Stats ---
def query_range(start_unix, end_unix):
    conn = get_conn()
    rows = conn.execute(
        "SELECT timestamp_unix, glucose_mg_dl FROM glucose WHERE timestamp_unix BETWEEN ? AND ? ORDER BY timestamp_unix",
        (start_unix, end_unix)
    ).fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]


def compute_stats(rows):
    if not rows:
        return {}
    values = [gl for _, gl in rows]
    mean = sum(values) / len(values)
    sd = math.sqrt(sum((v - mean)**2 for v in values) / len(values)) if len(values) > 1 else 0
    cv = (sd / mean * 100) if mean else 0
    gmi = 3.31 + 0.02392 * mean
    tir = sum(1 for v in values if 70 <= v <= 180) / len(values) * 100
    tbr = sum(1 for v in values if v < 70) / len(values) * 100
    tar = sum(1 for v in values if v > 180) / len(values) * 100
    return {
        "total_points": len(values),
        "mean_mg_dl": round(mean, 1),
        "sd_mg_dl": round(sd, 1),
        "cv_percent": round(cv, 1),
        "gmi": round(gmi, 1),
        "tir_percent": round(tir, 1),
        "tbr_percent": round(tbr, 1),
        "tar_percent": round(tar, 1)
    }


# --- Endpoints ---
@app.post("/import")
async def import_csv(file: UploadFile = File(...)):
    content = await file.read()
    parsed = parse_clarity_csv_content(content)
    if not parsed:
        raise HTTPException(400, "No data parsed from file")
    result = insert_glucose_rows(parsed)
    return {"filename": file.filename, "result": result}


@app.get("/glucose/latest")
def latest(hours: int = 24):
    now = int(datetime.now(timezone.utc).timestamp())
    rows = query_range(now - hours * 3600, now)
    return {"count": len(rows), "data": [{"ts": ts, "glucose": gl} for ts, gl in rows]}


@app.get("/glucose/range")
def range_query(start: str, end: str):
    start_unix = int(start) if start.isdigit() else parse_timestamp_to_unix(start)
    end_unix = int(end) if end.isdigit() else parse_timestamp_to_unix(end)
    rows = query_range(start_unix, end_unix)
    return {"count": len(rows), "data": [{"ts": ts, "glucose": gl} for ts, gl in rows]}


@app.get("/glucose/stats")
def stats(days: int = 14):
    now = int(datetime.now(timezone.utc).timestamp())
    rows = query_range(now - days * 86400, now)
    return {"days": days, "stats": compute_stats(rows)}


@app.get("/mcp")
def mcp():
    return {
        "id": "stelo-workaround-mcp",
        "name": "Dexcom Stelo Workaround MCP",
        "description": "Imports Dexcom Stelo/Clarity CSV exports into SQLite for glucose tracking",
        "endpoints": ["/import", "/glucose/latest", "/glucose/range", "/glucose/stats"]
    }


@app.get("/health")
def health():
    return {"status": "ok", "db_path": DB_PATH}
