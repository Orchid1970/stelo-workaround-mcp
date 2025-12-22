import os
import csv
import io
import aiosqlite
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

DB_PATH = os.getenv("DB_PATH", "/data/glucose.db")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield

app = FastAPI(title="Stelo Workaround MCP", lifespan=lifespan)

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        # Glucose readings (EGV events)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS glucose_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                glucose_value INTEGER,
                rate_of_change REAL,
                transmitter_time INTEGER,
                transmitter_id TEXT,
                source_device TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, transmitter_time)
            )
        """)
        # Activities
        await db.execute("""
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                duration TEXT,
                source_device TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, duration)
            )
        """)
        # Alerts
        await db.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                alert_type TEXT,
                glucose_value INTEGER,
                duration TEXT,
                source_device TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Insulin logs
        await db.execute("""
            CREATE TABLE IF NOT EXISTS insulin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                insulin_value REAL,
                source_device TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, insulin_value)
            )
        """)
        # Carb logs
        await db.execute("""
            CREATE TABLE IF NOT EXISTS carb_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                carb_value REAL,
                source_device TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, carb_value)
            )
        """)
        await db.commit()

@app.get("/health")
async def health():
    return {"status": "ok", "db_path": DB_PATH}

@app.post("/import")
async def import_csv(file: UploadFile = File(...)):
    """Import Dexcom Clarity CSV export with all event types."""
    content = await file.read()
    decoded = content.decode("utf-8-sig")  # Handle BOM
    reader = csv.DictReader(io.StringIO(decoded))
    
    stats = {"glucose": 0, "activities": 0, "alerts": 0, "insulin": 0, "carbs": 0, "skipped": 0}
    
    async with aiosqlite.connect(DB_PATH) as db:
        for row in reader:
            event_type = row.get("Event Type", "").strip()
            timestamp = row.get("Timestamp (YYYY-MM-DDThh:mm:ss)", "").strip()
            
            # Skip metadata rows
            if event_type in ["FirstName", "LastName", "Device", ""]:
                stats["skipped"] += 1
                continue
            
            source_device = row.get("Source Device ID", "").strip()
            
            # EGV - Glucose readings
            if event_type == "EGV":
                glucose = row.get("Glucose Value (mg/dL)", "").strip()
                if glucose:
                    rate = row.get("Glucose Rate of Change (mg/dL/min)", "").strip()
                    trans_time = row.get("Transmitter Time (Long Integer)", "").strip()
                    trans_id = row.get("Transmitter ID", "").strip()
                    try:
                        await db.execute("""
                            INSERT OR IGNORE INTO glucose_readings 
                            (timestamp, glucose_value, rate_of_change, transmitter_time, transmitter_id, source_device)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            timestamp,
                            int(glucose) if glucose else None,
                            float(rate) if rate else None,
                            int(trans_time) if trans_time else None,
                            trans_id or None,
                            source_device or None
                        ))
                        stats["glucose"] += 1
                    except Exception:
                        stats["skipped"] += 1
            
            # Activity events
            elif event_type == "Activity":
                duration = row.get("Duration (hh:mm:ss)", "").strip()
                try:
                    await db.execute("""
                        INSERT OR IGNORE INTO activities (timestamp, duration, source_device)
                        VALUES (?, ?, ?)
                    """, (timestamp, duration or None, source_device or None))
                    stats["activities"] += 1
                except Exception:
                    stats["skipped"] += 1
            
            # Alert events
            elif event_type == "Alert":
                alert_subtype = row.get("Event Subtype", "").strip()
                glucose = row.get("Glucose Value (mg/dL)", "").strip()
                duration = row.get("Duration (hh:mm:ss)", "").strip()
                try:
                    await db.execute("""
                        INSERT INTO alerts (timestamp, alert_type, glucose_value, duration, source_device)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        timestamp or None,
                        alert_subtype or None,
                        int(glucose) if glucose else None,
                        duration or None,
                        source_device or None
                    ))
                    stats["alerts"] += 1
                except Exception:
                    stats["skipped"] += 1
            
            # Insulin logging
            insulin = row.get("Insulin Value (u)", "").strip()
            if insulin and timestamp:
                try:
                    await db.execute("""
                        INSERT OR IGNORE INTO insulin_logs (timestamp, insulin_value, source_device)
                        VALUES (?, ?, ?)
                    """, (timestamp, float(insulin), source_device or None))
                    stats["insulin"] += 1
                except Exception:
                    pass
            
            # Carb logging
            carbs = row.get("Carb Value (grams)", "").strip()
            if carbs and timestamp:
                try:
                    await db.execute("""
                        INSERT OR IGNORE INTO carb_logs (timestamp, carb_value, source_device)
                        VALUES (?, ?, ?)
                    """, (timestamp, float(carbs), source_device or None))
                    stats["carbs"] += 1
                except Exception:
                    pass
        
        await db.commit()
    
    return {"status": "ok", "imported": stats}

@app.get("/glucose/latest")
async def get_latest_glucose(hours: int = 24):
    """Get glucose readings from the last N hours."""
    cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT timestamp, glucose_value, rate_of_change, transmitter_id
            FROM glucose_readings 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC
        """, (cutoff,))
        rows = await cursor.fetchall()
        data = [dict(r) for r in rows]
    return {"count": len(data), "data": data}

@app.get("/glucose/range")
async def get_glucose_range(start: str, end: str):
    """Get glucose readings within a date range (ISO format)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT timestamp, glucose_value, rate_of_change, transmitter_id
            FROM glucose_readings 
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
        """, (start, end))
        rows = await cursor.fetchall()
        data = [dict(r) for r in rows]
    return {"count": len(data), "data": data}

@app.get("/glucose/stats")
async def get_glucose_stats(days: int = 7):
    """Get glucose statistics for the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            SELECT 
                COUNT(*) as reading_count,
                AVG(glucose_value) as avg_glucose,
                MIN(glucose_value) as min_glucose,
                MAX(glucose_value) as max_glucose,
                AVG(CASE WHEN glucose_value BETWEEN 70 AND 180 THEN 1.0 ELSE 0.0 END) * 100 as time_in_range_pct
            FROM glucose_readings 
            WHERE timestamp >= ? AND glucose_value IS NOT NULL
        """, (cutoff,))
        row = await cursor.fetchone()
        if row:
            return {
                "days": days,
                "reading_count": row[0],
                "avg_glucose": round(row[1], 1) if row[1] else None,
                "min_glucose": row[2],
                "max_glucose": row[3],
                "time_in_range_pct": round(row[4], 1) if row[4] else None
            }
    return {"days": days, "reading_count": 0}

@app.get("/activities/latest")
async def get_latest_activities(days: int = 7):
    """Get activities from the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT timestamp, duration, source_device
            FROM activities 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC
        """, (cutoff,))
        rows = await cursor.fetchall()
        data = [dict(r) for r in rows]
    return {"count": len(data), "data": data}

@app.get("/alerts/latest")
async def get_latest_alerts(days: int = 7):
    """Get alerts from the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT timestamp, alert_type, glucose_value, duration
            FROM alerts 
            WHERE timestamp >= ? OR timestamp IS NULL
            ORDER BY timestamp DESC
        """, (cutoff,))
        rows = await cursor.fetchall()
        data = [dict(r) for r in rows]
    return {"count": len(data), "data": data}

@app.get("/insulin/latest")
async def get_latest_insulin(days: int = 7):
    """Get insulin logs from the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT timestamp, insulin_value
            FROM insulin_logs 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC
        """, (cutoff,))
        rows = await cursor.fetchall()
        data = [dict(r) for r in rows]
    return {"count": len(data), "data": data}

@app.get("/carbs/latest")
async def get_latest_carbs(days: int = 7):
    """Get carb logs from the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT timestamp, carb_value
            FROM carb_logs 
            WHERE timestamp >= ? 
            ORDER BY timestamp DESC
        """, (cutoff,))
        rows = await cursor.fetchall()
        data = [dict(r) for r in rows]
    return {"count": len(data), "data": data}

@app.get("/summary")
async def get_summary(days: int = 7):
    """Get a full summary of all data for the last N days."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        # Glucose stats
        cursor = await db.execute("""
            SELECT COUNT(*), AVG(glucose_value), MIN(glucose_value), MAX(glucose_value)
            FROM glucose_readings WHERE timestamp >= ?
        """, (cutoff,))
        g = await cursor.fetchone()
        
        # Activity count
        cursor = await db.execute("SELECT COUNT(*) FROM activities WHERE timestamp >= ?", (cutoff,))
        a = await cursor.fetchone()
        
        # Alert count
        cursor = await db.execute("SELECT COUNT(*) FROM alerts WHERE timestamp >= ? OR timestamp IS NULL", (cutoff,))
        al = await cursor.fetchone()
        
    return {
        "days": days,
        "glucose": {
            "readings": g[0],
            "avg": round(g[1], 1) if g[1] else None,
            "min": g[2],
            "max": g[3]
        },
        "activities": a[0],
        "alerts": al[0]
    }

# MCP endpoint for Simtheory
@app.post("/mcp")
@app.get("/mcp")
async def mcp_handler():
    """MCP protocol endpoint."""
    return {
        "name": "stelo-workaround",
        "version": "2.0.0",
        "description": "Dexcom Stelo glucose data via Clarity CSV import",
        "tools": [
            {"name": "get_latest_glucose", "description": "Get recent glucose readings"},
            {"name": "get_glucose_range", "description": "Get glucose readings in date range"},
            {"name": "get_glucose_stats", "description": "Get glucose statistics"},
            {"name": "get_latest_activities", "description": "Get recent activities"},
            {"name": "get_latest_alerts", "description": "Get recent alerts"},
            {"name": "get_summary", "description": "Get full health summary"}
        ]
    }
