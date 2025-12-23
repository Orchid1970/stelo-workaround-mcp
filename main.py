"""
Stelo Glucose MCP Server - Workaround for Dexcom Stelo
Uploads Dexcom Clarity CSV exports and provides glucose data via MCP tools.
Version: 2.2.0 - Fixed MCP response format and startup
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional
import aiosqlite
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path - use /data for Railway volume persistence
DB_PATH = os.environ.get("DB_PATH", "/data/stelo.db")

logger.info(f"Starting Stelo MCP v2.2.0")
logger.info(f"Database path: {DB_PATH}")

# Ensure data directory exists (sync - runs at import time)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize FastAPI and MCP
app = FastAPI(title="Stelo Glucose MCP", version="2.2.0")
mcp = FastMCP("Stelo Glucose")

# Flag to track if migrations have run
_migrations_done = False

async def ensure_db_ready():
    """Ensure database is initialized. Called on first request."""
    global _migrations_done
    if not _migrations_done:
        from migrations import run_migrations
        await run_migrations(DB_PATH)
        logger.info("Database initialized and migrations complete")
        _migrations_done = True


# ============== FastAPI Endpoints ==============

@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM glucose_readings")
            count = (await cursor.fetchone())[0]
        return {"status": "healthy", "version": "2.2.0", "glucose_readings_count": count}
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "error", "message": str(e)}


@app.post("/upload/clarity")
async def upload_clarity_csv(file: UploadFile = File(...)):
    """Upload a Dexcom Clarity CSV export."""
    await ensure_db_ready()
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    content = await file.read()
    content_str = content.decode('utf-8')
    
    # Calculate hash of file content
    file_hash = hashlib.sha256(content).hexdigest()[:16]
    
    try:
        # Parse CSV - Clarity exports have metadata rows at top
        lines = content_str.strip().split('\n')
        
        # Find the header row (contains "Timestamp" or "Device Timestamp")
        header_idx = 0
        for i, line in enumerate(lines):
            if 'Timestamp' in line and 'Glucose' in line:
                header_idx = i
                break
        
        # Parse from header row
        from io import StringIO
        csv_data = '\n'.join(lines[header_idx:])
        df = pd.read_csv(StringIO(csv_data))
        
        # Normalize column names
        df.columns = df.columns.str.strip()
        
        # Find timestamp and glucose columns
        ts_col = None
        glucose_col = None
        
        for col in df.columns:
            if 'Timestamp' in col:
                ts_col = col
            if 'Glucose' in col and 'mg/dL' in col:
                glucose_col = col
        
        if not ts_col or not glucose_col:
            raise HTTPException(status_code=400, detail=f"Could not find required columns. Found: {list(df.columns)}")
        
        # Insert readings
        inserted = 0
        duplicates = 0
        
        async with aiosqlite.connect(DB_PATH) as db:
            for _, row in df.iterrows():
                ts = row[ts_col]
                glucose = row[glucose_col]
                
                # Skip empty readings
                if pd.isna(glucose) or pd.isna(ts):
                    continue
                
                # Create unique hash for this reading
                reading_hash = hashlib.sha256(f"{ts}:{glucose}".encode()).hexdigest()[:16]
                
                # Check for duplicate
                cursor = await db.execute(
                    "SELECT id FROM glucose_readings WHERE data_hash = ?",
                    (reading_hash,)
                )
                if await cursor.fetchone():
                    duplicates += 1
                    continue
                
                # Insert new reading
                try:
                    await db.execute(
                        """INSERT INTO glucose_readings (timestamp, glucose_mg_dl, source, data_hash)
                           VALUES (?, ?, ?, ?)""",
                        (str(ts), int(float(glucose)), 'clarity_csv', reading_hash)
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning(f"Failed to insert reading: {e}")
            
            await db.commit()
        
        return {
            "status": "success",
            "file": file.filename,
            "readings_inserted": inserted,
            "duplicates_skipped": duplicates
        }
        
    except Exception as e:
        logger.error(f"Error processing CSV: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============== MCP Tools ==============

@mcp.tool()
async def get_latest_glucose(hours: int = 24, limit: int = 100) -> str:
    """
    Get the latest glucose readings.
    
    Args:
        hours: Number of hours to look back (default 24)
        limit: Maximum number of readings to return (default 100)
    
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
            
            result = {
                "count": len(readings),
                "readings": readings
            }
            
            return json.dumps(result, indent=2)
            
    except Exception as e:
        logger.error(f"Error fetching glucose: {e}")
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_summary(days: int = 7) -> str:
    """
    Get a summary of glucose statistics over a period.
    
    Args:
        days: Number of days to summarize (default 7)
    
    Returns:
        JSON string with summary statistics including average, min, max, time in range
    """
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            # Get basic stats
            cursor = await db.execute(
                """SELECT 
                       COUNT(*) as count,
                       AVG(glucose_mg_dl) as avg,
                       MIN(glucose_mg_dl) as min,
                       MAX(glucose_mg_dl) as max
                   FROM glucose_readings"""
            )
            row = await cursor.fetchone()
            
            # Calculate time in range (70-180 mg/dL)
            cursor = await db.execute(
                """SELECT COUNT(*) FROM glucose_readings 
                   WHERE glucose_mg_dl BETWEEN 70 AND 180"""
            )
            in_range = (await cursor.fetchone())[0]
            
            # Time below range
            cursor = await db.execute(
                "SELECT COUNT(*) FROM glucose_readings WHERE glucose_mg_dl < 70"
            )
            below_range = (await cursor.fetchone())[0]
            
            # Time above range
            cursor = await db.execute(
                "SELECT COUNT(*) FROM glucose_readings WHERE glucose_mg_dl > 180"
            )
            above_range = (await cursor.fetchone())[0]
            
            total = row[0] if row[0] > 0 else 1
            
            # Get insulin entries count
            cursor = await db.execute("SELECT COUNT(*) FROM insulin_entries")
            insulin_count = (await cursor.fetchone())[0]
            
            result = {
                "period_days": days,
                "total_readings": row[0],
                "average_glucose": round(row[1], 1) if row[1] else 0,
                "min_glucose": row[2],
                "max_glucose": row[3],
                "time_in_range_pct": round(in_range / total * 100, 1),
                "time_below_range_pct": round(below_range / total * 100, 1),
                "time_above_range_pct": round(above_range / total * 100, 1),
                "insulin_entries": insulin_count
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
            
            # Calculate daily stats
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


# Mount MCP server
app.mount("/mcp", mcp.streamable_http_app())
