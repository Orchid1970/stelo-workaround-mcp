"""
Stelo Workaround MCP - FastAPI service for Dexcom Stelo/Clarity CSV imports
with MCP endpoints for Simtheory.ai
"""
import os
import io
import csv
import sqlite3
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional
from contextlib import contextmanager

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Import migrations
from migrations import run_migrations, check_and_add_data_hash_column

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DB_PATH = os.environ.get("DB_PATH", "/data/stelo.db")

app = FastAPI(
    title="Stelo Workaround MCP",
    description="Import Dexcom Stelo/Clarity CSV exports into SQLite with MCP endpoints",
    version="1.0.0"
)

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    """Initialize the database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create glucose_readings table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS glucose_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            glucose_value INTEGER NOT NULL,
            rate_of_change REAL,
            transmitter_id TEXT,
            data_hash TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON glucose_readings(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transmitter ON glucose_readings(transmitter_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp_transmitter ON glucose_readings(timestamp, transmitter_id)")
    
    conn.commit()
    conn.close()
    
    # Run migrations to add any new columns
    run_migrations(DB_PATH)
    
    logger.info(f"Database initialized at {DB_PATH}")

@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    init_db()

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "database": DB_PATH}

# CSV Upload endpoint for Clarity exports
@app.post("/upload/clarity")
async def upload_clarity(file: UploadFile = File(...)):
    """
    Upload a Dexcom Clarity CSV export file.
    Parses the CSV and imports glucose readings into the database.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        contents = await file.read()
        decoded = contents.decode('utf-8-sig')  # Handle BOM
        
        reader = csv.DictReader(io.StringIO(decoded))
        
        # Detect column names (Clarity exports have specific column names)
        fieldnames = reader.fieldnames
        
        # Map possible column names
        timestamp_col = None
        glucose_col = None
        rate_col = None
        transmitter_col = None
        event_type_col = None
        
        for col in fieldnames:
            col_lower = col.lower()
            if 'timestamp' in col_lower:
                timestamp_col = col
            elif 'glucose' in col_lower and 'value' in col_lower:
                glucose_col = col
            elif 'rate' in col_lower:
                rate_col = col
            elif 'transmitter' in col_lower and 'id' in col_lower:
                transmitter_col = col
            elif col_lower == 'event type' or col_lower == 'event_type':
                event_type_col = col
        
        if not timestamp_col or not glucose_col:
            raise HTTPException(
                status_code=400, 
                detail=f"Could not find required columns. Found: {fieldnames}"
            )
        
        logger.info(f"CSV columns detected: timestamp={timestamp_col}, glucose={glucose_col}, transmitter={transmitter_col}")
        
        inserted = 0
        duplicates = 0
        transmitter_ids = set()
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            for row in reader:
                # Skip non-EGV rows (header rows, calibration, etc.)
                if event_type_col and row.get(event_type_col) != 'EGV':
                    continue
                
                timestamp = row.get(timestamp_col, '').strip()
                glucose_str = row.get(glucose_col, '').strip()
                
                # Skip rows without valid data
                if not timestamp or not glucose_str:
                    continue
                
                # Parse glucose value
                try:
                    glucose_value = int(glucose_str)
                except ValueError:
                    continue
                
                # Get optional fields
                rate_of_change = None
                if rate_col and row.get(rate_col):
                    try:
                        rate_of_change = float(row.get(rate_col))
                    except ValueError:
                        pass
                
                transmitter_id = None
                if transmitter_col and row.get(transmitter_col):
                    transmitter_id = str(row.get(transmitter_col)).strip()
                    # Clean up transmitter ID (remove .0 if present from float conversion)
                    if transmitter_id.endswith('.0'):
                        transmitter_id = transmitter_id[:-2]
                    transmitter_ids.add(transmitter_id)
                
                # Check for duplicate based on timestamp and transmitter_id only
                # This fixes the issue where data_hash was marking all overlapping 
                # historical data as duplicates
                cursor.execute("""
                    SELECT COUNT(*) FROM glucose_readings 
                    WHERE timestamp = ? AND transmitter_id = ?
                """, (timestamp, transmitter_id))
                
                if cursor.fetchone()[0] > 0:
                    duplicates += 1
                    continue
                
                # Insert new reading
                cursor.execute("""
                    INSERT INTO glucose_readings 
                    (timestamp, glucose_value, rate_of_change, transmitter_id)
                    VALUES (?, ?, ?, ?)
                """, (timestamp, glucose_value, rate_of_change, transmitter_id))
                
                inserted += 1
            
            conn.commit()
        
        logger.info(f"Upload complete: {inserted} new readings, {duplicates} duplicates, {len(transmitter_ids)} unique sensors")
        
        return {
            "status": "success",
            "inserted": inserted,
            "duplicates": duplicates,
            "transmitter_ids": list(transmitter_ids)
        }
        
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# MCP Endpoint for JSON-RPC
class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[dict] = None
    id: Optional[str] = None

@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP JSON-RPC endpoint for Simtheory.ai integration"""
    logger.info(f"MCP JSON-RPC: method={request.method}, id={request.id}")
    
    if request.method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request.id,
            "result": {
                "tools": [
                    {
                        "name": "get_latest_glucose",
                        "description": "Get the most recent glucose readings",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "hours": {
                                    "type": "integer",
                                    "description": "Number of hours to look back (default: 24)",
                                    "default": 24
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Maximum number of readings to return (default: 100)",
                                    "default": 100
                                }
                            }
                        }
                    },
                    {
                        "name": "get_glucose_stats",
                        "description": "Get glucose statistics for a time period",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "days": {
                                    "type": "integer",
                                    "description": "Number of days to analyze (default: 7)",
                                    "default": 7
                                }
                            }
                        }
                    },
                    {
                        "name": "get_summary",
                        "description": "Get a natural language summary of glucose data",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "days": {
                                    "type": "integer",
                                    "description": "Number of days to summarize (default: 7)",
                                    "default": 7
                                }
                            }
                        }
                    }
                ]
            }
        }
    
    elif request.method == "tools/call":
        tool_name = request.params.get("name") if request.params else None
        tool_args = request.params.get("arguments", {}) if request.params else {}
        
        logger.info(f"Tool call: {tool_name} with args: {tool_args}")
        
        if tool_name == "get_latest_glucose":
            hours = tool_args.get("hours", 24)
            limit = tool_args.get("limit", 100)
            
            cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
            
            with get_db() as conn:
                cursor = conn.execute("""
                    SELECT timestamp, glucose_value, rate_of_change, transmitter_id
                    FROM glucose_readings
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (cutoff, limit))
                
                readings = [
                    {
                        "timestamp": row["timestamp"],
                        "glucose": row["glucose_value"],
                        "rate_of_change": row["rate_of_change"],
                        "transmitter_id": row["transmitter_id"]
                    }
                    for row in cursor.fetchall()
                ]
            
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Found {len(readings)} readings in the last {hours} hours:\n" + 
                                   "\n".join([f"  {r['timestamp']}: {r['glucose']} mg/dL" for r in readings[:10]]) +
                                   (f"\n  ... and {len(readings) - 10} more" if len(readings) > 10 else "")
                        }
                    ],
                    "data": readings
                }
            }
        
        elif tool_name == "get_glucose_stats":
            days = tool_args.get("days", 7)
            cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            with get_db() as conn:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as count,
                        AVG(glucose_value) as avg,
                        MIN(glucose_value) as min,
                        MAX(glucose_value) as max,
                        MIN(timestamp) as first_reading,
                        MAX(timestamp) as last_reading
                    FROM glucose_readings
                    WHERE timestamp >= ?
                """, (cutoff,))
                
                row = cursor.fetchone()
                
                # Calculate time in range (70-180 mg/dL)
                cursor2 = conn.execute("""
                    SELECT 
                        COUNT(*) as in_range
                    FROM glucose_readings
                    WHERE timestamp >= ? AND glucose_value BETWEEN 70 AND 180
                """, (cutoff,))
                in_range = cursor2.fetchone()["in_range"]
                
                # Calculate time below range (<70)
                cursor3 = conn.execute("""
                    SELECT COUNT(*) as below
                    FROM glucose_readings
                    WHERE timestamp >= ? AND glucose_value < 70
                """, (cutoff,))
                below_range = cursor3.fetchone()["below"]
                
                # Calculate time above range (>180)
                cursor4 = conn.execute("""
                    SELECT COUNT(*) as above
                    FROM glucose_readings
                    WHERE timestamp >= ? AND glucose_value > 180
                """, (cutoff,))
                above_range = cursor4.fetchone()["above"]
            
            total = row["count"] if row["count"] else 1
            stats = {
                "period_days": days,
                "total_readings": row["count"],
                "average_glucose": round(row["avg"], 1) if row["avg"] else None,
                "min_glucose": row["min"],
                "max_glucose": row["max"],
                "first_reading": row["first_reading"],
                "last_reading": row["last_reading"],
                "time_in_range_pct": round(in_range / total * 100, 1),
                "time_below_range_pct": round(below_range / total * 100, 1),
                "time_above_range_pct": round(above_range / total * 100, 1)
            }
            
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"Glucose Statistics ({days} days):\n" +
                                   f"  Average: {stats['average_glucose']} mg/dL\n" +
                                   f"  Range: {stats['min_glucose']} - {stats['max_glucose']} mg/dL\n" +
                                   f"  Time in Range (70-180): {stats['time_in_range_pct']}%\n" +
                                   f"  Time Below (<70): {stats['time_below_range_pct']}%\n" +
                                   f"  Time Above (>180): {stats['time_above_range_pct']}%\n" +
                                   f"  Total Readings: {stats['total_readings']}"
                        }
                    ],
                    "data": stats
                }
            }
        
        elif tool_name == "get_summary":
            days = tool_args.get("days", 7)
            cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            with get_db() as conn:
                cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as count,
                        AVG(glucose_value) as avg,
                        MIN(glucose_value) as min,
                        MAX(glucose_value) as max
                    FROM glucose_readings
                    WHERE timestamp >= ?
                """, (cutoff,))
                row = cursor.fetchone()
                
                # Time in range
                cursor2 = conn.execute("""
                    SELECT COUNT(*) as in_range
                    FROM glucose_readings
                    WHERE timestamp >= ? AND glucose_value BETWEEN 70 AND 180
                """, (cutoff,))
                in_range = cursor2.fetchone()["in_range"]
            
            total = row["count"] if row["count"] else 1
            tir_pct = round(in_range / total * 100, 1)
            avg = round(row["avg"], 1) if row["avg"] else 0
            
            # Generate summary
            if tir_pct >= 70:
                control_status = "excellent glucose control"
            elif tir_pct >= 50:
                control_status = "moderate glucose control"
            else:
                control_status = "glucose levels that need attention"
            
            summary = f"""Over the past {days} days, you've had {control_status} with {tir_pct}% time in range (70-180 mg/dL).

Your average glucose was {avg} mg/dL, ranging from {row['min']} to {row['max']} mg/dL across {row['count']} readings.

{"Great job maintaining stable levels!" if tir_pct >= 70 else "Consider reviewing meal timing and activity patterns to improve time in range."}"""
            
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": summary
                        }
                    ]
                }
            }
        
        else:
            return {
                "jsonrpc": "2.0",
                "id": request.id,
                "error": {
                    "code": -32601,
                    "message": f"Unknown tool: {tool_name}"
                }
            }
    
    else:
        return {
            "jsonrpc": "2.0",
            "id": request.id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {request.method}"
            }
        }

# Manual database query endpoint (for debugging)
@app.get("/debug/readings")
async def debug_readings(limit: int = 10):
    """Debug endpoint to view recent readings"""
    with get_db() as conn:
        cursor = conn.execute("""
            SELECT * FROM glucose_readings
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,))
        
        readings = [dict(row) for row in cursor.fetchall()]
    
    return {"readings": readings}

@app.get("/debug/count")
async def debug_count():
    """Debug endpoint to get total reading count"""
    with get_db() as conn:
        cursor = conn.execute("SELECT COUNT(*) as count FROM glucose_readings")
        count = cursor.fetchone()["count"]
        
        cursor2 = conn.execute("""
            SELECT MIN(timestamp) as first, MAX(timestamp) as last 
            FROM glucose_readings
        """)
        row = cursor2.fetchone()
    
    return {
        "total_readings": count,
        "first_reading": row["first"],
        "last_reading": row["last"]
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
