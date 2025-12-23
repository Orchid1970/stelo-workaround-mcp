"""
Stelo Glucose MCP Server - Workaround for Dexcom Stelo
Uploads Dexcom Clarity CSV exports and provides glucose data via MCP tools.
Version: 2.2.4 - Diagnostic version to debug MCP mounting
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional
import aiosqlite
from fastapi import FastAPI, HTTPException, UploadFile, File, Request, Response
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database path - use /data for Railway volume persistence
DB_PATH = os.environ.get("DB_PATH", "/data/stelo.db")

logger.info(f"Starting Stelo MCP v2.2.4 (diagnostic)")
logger.info(f"Database path: {DB_PATH}")

# Ensure data directory exists (sync - runs at import time)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize FastAPI
app = FastAPI(title="Stelo Glucose MCP", version="2.2.4")
mcp = FastMCP("Stelo Glucose")

# Flag to track if migrations have run
_migrations_done = False

async def ensure_db_ready():
    """Ensure database is initialized. Called lazily on first request."""
    global _migrations_done
    if not _migrations_done:
        from migrations import run_migrations
        run_migrations(DB_PATH)
        logger.info("Database initialized and migrations complete")
        _migrations_done = True


# ============== MCP Tools (define BEFORE getting the app) ==============

@mcp.tool()
async def get_latest_glucose(hours: int = 24, limit: int = 50) -> str:
    """
    Get the most recent glucose readings.
    
    Args:
        hours: Number of hours to look back (default 24)
        limit: Maximum number of readings to return (default 50)
    
    Returns:
        JSON string with glucose readings
    """
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT timestamp, glucose_mg_dl 
                   FROM glucose_readings 
                   ORDER BY timestamp DESC 
                   LIMIT ?""",
                (limit,)
            )
            rows = await cursor.fetchall()
            readings = [{"timestamp": row["timestamp"], "glucose_mg_dl": row["glucose_mg_dl"]} for row in rows]
            return json.dumps({"readings": readings, "count": len(readings)}, indent=2)
    except Exception as e:
        logger.error(f"Error fetching glucose: {e}")
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_glucose_stats(days: int = 7) -> str:
    """
    Get glucose statistics for the specified number of days.
    
    Args:
        days: Number of days to analyze (default 7)
    
    Returns:
        JSON string with statistics including average, min, max, std dev, time in range
    """
    try:
        await ensure_db_ready()
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT glucose_mg_dl FROM glucose_readings 
                   WHERE timestamp >= ?""",
                (cutoff,)
            )
            rows = await cursor.fetchall()
            
            if not rows:
                return json.dumps({"error": "No readings found for this period"})
            
            values = [row["glucose_mg_dl"] for row in rows]
            avg = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)
            in_range = sum(1 for v in values if 70 <= v <= 180)
            below_range = sum(1 for v in values if v < 70)
            above_range = sum(1 for v in values if v > 180)
            variance = sum((v - avg) ** 2 for v in values) / len(values)
            std_dev = variance ** 0.5
            cv = (std_dev / avg) * 100 if avg > 0 else 0
            
            stats = {
                "period_days": days,
                "total_readings": len(values),
                "average_mg_dl": round(avg, 1),
                "min_mg_dl": min_val,
                "max_mg_dl": max_val,
                "std_dev": round(std_dev, 1),
                "cv_percent": round(cv, 1),
                "time_in_range_percent": round(in_range / len(values) * 100, 1),
                "time_below_range_percent": round(below_range / len(values) * 100, 1),
                "time_above_range_percent": round(above_range / len(values) * 100, 1),
                "readings_in_range": in_range,
                "readings_below_70": below_range,
                "readings_above_180": above_range
            }
            return json.dumps(stats, indent=2)
    except Exception as e:
        logger.error(f"Error calculating stats: {e}")
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_summary(days: int = 14) -> str:
    """
    Get a comprehensive summary of glucose data and logged entries.
    
    Args:
        days: Number of days to summarize (default 14)
    
    Returns:
        JSON string with summary including glucose stats, patterns, and logged entries
    """
    try:
        await ensure_db_ready()
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT timestamp, glucose_mg_dl FROM glucose_readings 
                   WHERE timestamp >= ? ORDER BY timestamp""",
                (cutoff,)
            )
            glucose_rows = await cursor.fetchall()
            
            cursor = await db.execute(
                """SELECT timestamp, units, insulin_type FROM insulin_entries 
                   WHERE timestamp >= ? ORDER BY timestamp""",
                (cutoff,)
            )
            insulin_rows = await cursor.fetchall()
            
            if not glucose_rows:
                return json.dumps({"error": "No glucose readings found for this period"})
            
            values = [row["glucose_mg_dl"] for row in glucose_rows]
            total = len(values)
            avg = sum(values) / total
            in_range = sum(1 for v in values if 70 <= v <= 180)
            below_range = sum(1 for v in values if v < 70)
            above_range = sum(1 for v in values if v > 180)
            first_reading = glucose_rows[0]["timestamp"]
            last_reading = glucose_rows[-1]["timestamp"]
            insulin_count = len(insulin_rows)
            total_insulin_units = sum(row["units"] for row in insulin_rows) if insulin_rows else 0
            
            result = {
                "period_days": days,
                "date_range": {"from": first_reading, "to": last_reading},
                "glucose": {
                    "total_readings": total,
                    "average_mg_dl": round(avg, 1),
                    "min_mg_dl": min(values),
                    "max_mg_dl": max(values),
                    "time_in_range_pct": round(in_range / total * 100, 1),
                    "time_below_range_pct": round(below_range / total * 100, 1),
                    "time_above_range_pct": round(above_range / total * 100, 1)
                },
                "insulin": {"entries": insulin_count, "total_units": round(total_insulin_units, 1)}
            }
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_glucose_by_date(date: str) -> str:
    """
    Get all glucose readings for a specific date.
    
    Args:
        date: Date in YYYY-MM-DD format
    
    Returns:
        JSON string with all readings for that date
    """
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT timestamp, glucose_mg_dl 
                   FROM glucose_readings 
                   WHERE timestamp LIKE ?
                   ORDER BY timestamp""",
                (f"{date}%",)
            )
            rows = await cursor.fetchall()
            readings = [{"timestamp": row["timestamp"], "glucose_mg_dl": row["glucose_mg_dl"]} for row in rows]
            
            if readings:
                values = [r["glucose_mg_dl"] for r in readings]
                stats = {
                    "date": date,
                    "count": len(readings),
                    "average": round(sum(values) / len(values), 1),
                    "min": min(values),
                    "max": max(values),
                    "readings": readings
                }
            else:
                stats = {"date": date, "count": 0, "message": "No readings found for this date"}
            return json.dumps(stats, indent=2)
    except Exception as e:
        logger.error(f"Error fetching by date: {e}")
        return json.dumps({"error": str(e)})


@mcp.tool()
async def log_insulin(units: float, insulin_type: str = "rapid", notes: str = "") -> str:
    """
    Log an insulin dose.
    
    Args:
        units: Number of insulin units
        insulin_type: Type of insulin (rapid, long, mixed)
        notes: Optional notes about the dose
    
    Returns:
        JSON string with confirmation of logged entry
    """
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            timestamp = datetime.now().isoformat()
            await db.execute(
                """INSERT INTO insulin_entries (timestamp, units, insulin_type, notes)
                   VALUES (?, ?, ?, ?)""",
                (timestamp, units, insulin_type, notes)
            )
            await db.commit()
            result = {
                "status": "success",
                "timestamp": timestamp,
                "units": units,
                "insulin_type": insulin_type,
                "notes": notes
            }
            return json.dumps(result, indent=2)
    except Exception as e:
        logger.error(f"Error logging insulin: {e}")
        return json.dumps({"error": str(e)})


# ============== Get MCP App and inspect it ==============
logger.info("Creating MCP streamable HTTP app...")
mcp_app = mcp.streamable_http_app()
logger.info(f"MCP app type: {type(mcp_app)}")
logger.info(f"MCP app: {mcp_app}")

# Try to inspect routes if it's a Starlette app
if hasattr(mcp_app, 'routes'):
    logger.info(f"MCP app routes: {mcp_app.routes}")
    for route in mcp_app.routes:
        logger.info(f"  Route: {route}")
        if hasattr(route, 'path'):
            logger.info(f"    Path: {route.path}")
        if hasattr(route, 'methods'):
            logger.info(f"    Methods: {route.methods}")


# ============== FastAPI Endpoints ==============

@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM glucose_readings")
            count = (await cursor.fetchone())[0]
        return {"status": "healthy", "version": "2.2.4", "glucose_readings_count": count}
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/debug/mcp-info")
async def debug_mcp_info():
    """Debug endpoint to show MCP app info."""
    info = {
        "mcp_app_type": str(type(mcp_app)),
        "mcp_app_str": str(mcp_app)[:500],
        "has_routes": hasattr(mcp_app, 'routes'),
        "routes": []
    }
    if hasattr(mcp_app, 'routes'):
        for route in mcp_app.routes:
            route_info = {"type": str(type(route))}
            if hasattr(route, 'path'):
                route_info["path"] = route.path
            if hasattr(route, 'methods'):
                route_info["methods"] = list(route.methods) if route.methods else None
            if hasattr(route, 'name'):
                route_info["name"] = route.name
            info["routes"].append(route_info)
    return info


@app.post("/upload/clarity")
async def upload_clarity_csv(file: UploadFile = File(...)):
    """Upload a Dexcom Clarity CSV export."""
    await ensure_db_ready()
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    content = await file.read()
    content_str = content.decode('utf-8')
    file_hash = hashlib.sha256(content).hexdigest()[:16]
    
    try:
        lines = content_str.strip().split('\n')
        header_idx = 0
        for i, line in enumerate(lines):
            if 'Timestamp' in line and 'Glucose' in line:
                header_idx = i
                break
        
        from io import StringIO
        csv_data = '\n'.join(lines[header_idx:])
        df = pd.read_csv(StringIO(csv_data))
        df.columns = df.columns.str.strip()
        
        ts_col = None
        glucose_col = None
        insulin_col = None
        carbs_col = None
        event_subtype_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'timestamp' in col_lower and ts_col is None:
                ts_col = col
            if 'glucose' in col_lower and 'mg' in col_lower:
                glucose_col = col
            if 'event subtype' in col_lower:
                event_subtype_col = col
            if 'insulin' in col_lower and 'value' in col_lower:
                insulin_col = col
            if 'carb' in col_lower and 'value' in col_lower:
                carbs_col = col
        
        if not ts_col:
            raise HTTPException(status_code=400, detail="Could not find timestamp column")
        
        inserted_glucose = 0
        inserted_insulin = 0
        inserted_carbs = 0
        skipped_duplicates = 0
        
        async with aiosqlite.connect(DB_PATH) as db:
            for _, row in df.iterrows():
                timestamp = str(row[ts_col]).strip()
                if timestamp == 'nan' or not timestamp:
                    continue
                
                if glucose_col and pd.notna(row.get(glucose_col)):
                    glucose_val = row[glucose_col]
                    if isinstance(glucose_val, (int, float)) and glucose_val > 0:
                        reading_hash = hashlib.sha256(f"{timestamp}:{glucose_val}:{file_hash}".encode()).hexdigest()[:16]
                        cursor = await db.execute(
                            "SELECT 1 FROM glucose_readings WHERE timestamp = ? AND glucose_mg_dl = ?",
                            (timestamp, int(glucose_val))
                        )
                        exists = await cursor.fetchone()
                        if not exists:
                            await db.execute(
                                """INSERT INTO glucose_readings (timestamp, glucose_mg_dl, data_hash)
                                   VALUES (?, ?, ?)""",
                                (timestamp, int(glucose_val), reading_hash)
                            )
                            inserted_glucose += 1
                        else:
                            skipped_duplicates += 1
                
                if insulin_col and pd.notna(row.get(insulin_col)):
                    insulin_val = row[insulin_col]
                    if isinstance(insulin_val, (int, float)) and insulin_val > 0:
                        cursor = await db.execute(
                            "SELECT 1 FROM insulin_entries WHERE timestamp = ? AND units = ?",
                            (timestamp, float(insulin_val))
                        )
                        exists = await cursor.fetchone()
                        if not exists:
                            insulin_type = "rapid"
                            if event_subtype_col and pd.notna(row.get(event_subtype_col)):
                                subtype = str(row[event_subtype_col]).lower()
                                if 'long' in subtype:
                                    insulin_type = "long"
                            await db.execute(
                                """INSERT INTO insulin_entries (timestamp, units, insulin_type, notes)
                                   VALUES (?, ?, ?, ?)""",
                                (timestamp, float(insulin_val), insulin_type, "From Clarity CSV")
                            )
                            inserted_insulin += 1
                
                if carbs_col and pd.notna(row.get(carbs_col)):
                    carbs_val = row[carbs_col]
                    if isinstance(carbs_val, (int, float)) and carbs_val > 0:
                        cursor = await db.execute(
                            "SELECT 1 FROM carb_entries WHERE timestamp = ? AND grams = ?",
                            (timestamp, int(carbs_val))
                        )
                        exists = await cursor.fetchone()
                        if not exists:
                            await db.execute(
                                """INSERT INTO carb_entries (timestamp, grams, notes)
                                   VALUES (?, ?, ?)""",
                                (timestamp, int(carbs_val), "From Clarity CSV")
                            )
                            inserted_carbs += 1
            
            await db.commit()
            cursor = await db.execute("SELECT COUNT(*) FROM glucose_readings")
            total = (await cursor.fetchone())[0]
        
        return {
            "status": "success",
            "file": file.filename,
            "inserted_glucose_readings": inserted_glucose,
            "inserted_insulin_entries": inserted_insulin,
            "inserted_carb_entries": inserted_carbs,
            "skipped_duplicates": skipped_duplicates,
            "total_glucose_readings": total
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing CSV: {str(e)}")


# ============== Mount MCP App ==============
logger.info("Mounting MCP app at /mcp...")
app.mount("/mcp", mcp_app)
logger.info("MCP app mounted.")

# Log all routes on the main app
logger.info("Main app routes:")
for route in app.routes:
    logger.info(f"  {route}")
