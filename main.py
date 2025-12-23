"""Stelo Workaround MCP - Dexcom Clarity CSV Import with MCP Protocol Support"""
import os
import json
import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import JSONResponse
import pandas as pd

# Import migrations
from migrations import run_migrations, check_and_add_data_hash_column

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = os.environ.get("DB_PATH", "/data/stelo.db")

# MCP Protocol version
MCP_VERSION = "2.1.0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    # Startup: Initialize database and run migrations
    logger.info(f"Starting Stelo MCP v{MCP_VERSION}")
    logger.info(f"Database path: {DB_PATH}")
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # Initialize database schema
    init_database()
    
    # Run migrations to ensure schema is up to date
    try:
        run_migrations(DB_PATH)
    except Exception as e:
        logger.warning(f"Migration system error: {e}, trying fallback")
        # Fallback: directly check and add column
        check_and_add_data_hash_column(DB_PATH)
    
    logger.info("Database initialized and migrations complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Stelo MCP")


app = FastAPI(
    title="Stelo Workaround MCP",
    version=MCP_VERSION,
    lifespan=lifespan
)


def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """Initialize database tables"""
    conn = get_db()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS glucose_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                glucose_value INTEGER,
                rate_of_change REAL,
                transmitter_id TEXT,
                data_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(timestamp, transmitter_id)
            );
            
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                activity_type TEXT,
                duration_minutes INTEGER,
                intensity TEXT,
                data_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                alert_type TEXT,
                glucose_value INTEGER,
                data_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS carbs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                carbs_grams INTEGER,
                data_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS insulin (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                insulin_type TEXT,
                units REAL,
                data_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_glucose_timestamp ON glucose_readings(timestamp);
            CREATE INDEX IF NOT EXISTS idx_glucose_hash ON glucose_readings(data_hash);
        """)
        conn.commit()
    finally:
        conn.close()


def compute_hash(row_data: str) -> str:
    """Compute hash for deduplication"""
    return hashlib.md5(row_data.encode()).hexdigest()


# ============ MCP Protocol Endpoints ============

@app.post("/mcp")
async def mcp_handler(request: Request):
    """Main MCP JSON-RPC handler"""
    try:
        body = await request.json()
    except:
        return JSONResponse({
            "jsonrpc": "2.0",
            "error": {"code": -32700, "message": "Parse error"},
            "id": None
        })
    
    method = body.get("method", "")
    params = body.get("params", {})
    req_id = body.get("id", 1)
    
    # Route to appropriate handler
    if method == "initialize":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "stelo-workaround-mcp", "version": MCP_VERSION}
            }
        })
    
    elif method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {
                        "name": "get_latest_glucose",
                        "description": "Get recent glucose readings",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "hours": {"type": "integer", "description": "Hours to look back", "default": 24},
                                "limit": {"type": "integer", "description": "Max readings", "default": 100}
                            }
                        }
                    },
                    {
                        "name": "get_glucose_range",
                        "description": "Get glucose readings in date range",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                                "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"}
                            },
                            "required": ["start_date", "end_date"]
                        }
                    },
                    {
                        "name": "get_glucose_stats",
                        "description": "Get glucose statistics",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "days": {"type": "integer", "description": "Days to analyze", "default": 14}
                            }
                        }
                    },
                    {
                        "name": "get_latest_activities",
                        "description": "Get recent activities",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "limit": {"type": "integer", "default": 50}
                            }
                        }
                    },
                    {
                        "name": "get_latest_alerts",
                        "description": "Get recent alerts",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "limit": {"type": "integer", "default": 50}
                            }
                        }
                    },
                    {
                        "name": "get_latest_carbs",
                        "description": "Get recent carb entries",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "limit": {"type": "integer", "default": 50}
                            }
                        }
                    },
                    {
                        "name": "get_latest_insulin",
                        "description": "Get recent insulin entries",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "limit": {"type": "integer", "default": 50}
                            }
                        }
                    },
                    {
                        "name": "get_summary",
                        "description": "Get full health summary",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "days": {"type": "integer", "default": 7}
                            }
                        }
                    }
                ]
            }
        })
    
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        result = await execute_tool(tool_name, arguments)
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"content": [result]}
        })
    
    else:
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        })


async def execute_tool(name: str, args: dict) -> str:
    """Execute a tool and return result"""
    conn = get_db()
    try:
        if name == "get_latest_glucose":
            hours = args.get("hours", 24)
            limit = args.get("limit", 100)
            cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
            cursor = conn.execute(
                "SELECT timestamp, glucose_value, rate_of_change, transmitter_id FROM glucose_readings WHERE timestamp > ? ORDER BY timestamp DESC LIMIT ?",
                (cutoff, limit)
            )
            rows = [dict(r) for r in cursor.fetchall()]
            return json.dumps({"count": len(rows), "data": rows}, indent=2)
        
        elif name == "get_glucose_range":
            start = args.get("start_date")
            end = args.get("end_date")
            cursor = conn.execute(
                "SELECT timestamp, glucose_value, rate_of_change, transmitter_id FROM glucose_readings WHERE date(timestamp) >= ? AND date(timestamp) <= ? ORDER BY timestamp DESC",
                (start, end)
            )
            rows = [dict(r) for r in cursor.fetchall()]
            return json.dumps({"count": len(rows), "start_date": start, "end_date": end, "data": rows}, indent=2)
        
        elif name == "get_glucose_stats":
            days = args.get("days", 14)
            cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
            cursor = conn.execute(
                "SELECT glucose_value FROM glucose_readings WHERE timestamp > ?",
                (cutoff,)
            )
            values = [r[0] for r in cursor.fetchall() if r[0] is not None]
            
            if not values:
                return json.dumps({"error": "No data available"}, indent=2)
            
            avg = sum(values) / len(values)
            std = (sum((v - avg) ** 2 for v in values) / len(values)) ** 0.5
            in_range = sum(1 for v in values if 70 <= v <= 180)
            below = sum(1 for v in values if v < 70)
            above = sum(1 for v in values if v > 180)
            
            return json.dumps({
                "days_analyzed": days,
                "total_readings": len(values),
                "average_glucose": round(avg, 1),
                "min_glucose": min(values),
                "max_glucose": max(values),
                "std_deviation": round(std, 1),
                "coefficient_of_variation": round((std / avg) * 100, 1) if avg else 0,
                "time_in_range_percent": round(in_range / len(values) * 100, 1),
                "time_below_range_percent": round(below / len(values) * 100, 1),
                "time_above_range_percent": round(above / len(values) * 100, 1)
            }, indent=2)
        
        elif name == "get_latest_activities":
            limit = args.get("limit", 50)
            cursor = conn.execute("SELECT * FROM activities ORDER BY timestamp DESC LIMIT ?", (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            return json.dumps({"count": len(rows), "data": rows}, indent=2)
        
        elif name == "get_latest_alerts":
            limit = args.get("limit", 50)
            cursor = conn.execute("SELECT * FROM alerts ORDER BY timestamp DESC LIMIT ?", (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            return json.dumps({"count": len(rows), "data": rows}, indent=2)
        
        elif name == "get_latest_carbs":
            limit = args.get("limit", 50)
            cursor = conn.execute("SELECT * FROM carbs ORDER BY timestamp DESC LIMIT ?", (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            return json.dumps({"count": len(rows), "data": rows}, indent=2)
        
        elif name == "get_latest_insulin":
            limit = args.get("limit", 50)
            cursor = conn.execute("SELECT * FROM insulin ORDER BY timestamp DESC LIMIT ?", (limit,))
            rows = [dict(r) for r in cursor.fetchall()]
            return json.dumps({"count": len(rows), "data": rows}, indent=2)
        
        elif name == "get_summary":
            days = args.get("days", 7)
            # Get glucose stats
            stats_result = await execute_tool("get_glucose_stats", {"days": days})
            stats = json.loads(stats_result)
            
            # Get counts of other data
            cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
            
            activities_count = conn.execute("SELECT COUNT(*) FROM activities WHERE timestamp > ?", (cutoff,)).fetchone()[0]
            alerts_count = conn.execute("SELECT COUNT(*) FROM alerts WHERE timestamp > ?", (cutoff,)).fetchone()[0]
            carbs_count = conn.execute("SELECT COUNT(*) FROM carbs WHERE timestamp > ?", (cutoff,)).fetchone()[0]
            insulin_count = conn.execute("SELECT COUNT(*) FROM insulin WHERE timestamp > ?", (cutoff,)).fetchone()[0]
            
            return json.dumps({
                "period_days": days,
                "glucose_stats": stats,
                "activities_count": activities_count,
                "alerts_count": alerts_count,
                "carbs_entries": carbs_count,
                "insulin_entries": insulin_count
            }, indent=2)
        
        else:
            return json.dumps({"error": f"Unknown tool: {name}"})
    
    finally:
        conn.close()


# ============ Import Endpoint ============

@app.post("/import")
async def import_clarity_csv(file: UploadFile = File(...)):
    """Import Dexcom Clarity CSV export"""
    logger.info(f"Importing file: {file.filename}")
    
    try:
        content = await file.read()
        content_str = content.decode('utf-8')
        
        # Parse CSV
        from io import StringIO
        df = pd.read_csv(StringIO(content_str), skiprows=find_header_row(content_str))
        
        conn = get_db()
        results = {
            "glucose_readings": 0,
            "activities": 0,
            "alerts": 0,
            "carbs": 0,
            "insulin": 0,
            "duplicates_skipped": 0,
            "errors": []
        }
        
        for idx, row in df.iterrows():
            try:
                row_hash = compute_hash(str(row.to_dict()))
                
                # Check for glucose reading
                if 'Glucose Value (mg/dL)' in df.columns and pd.notna(row.get('Glucose Value (mg/dL)')):
                    timestamp = parse_timestamp(row)
                    if timestamp:
                        try:
                            conn.execute(
                                "INSERT INTO glucose_readings (timestamp, glucose_value, rate_of_change, transmitter_id, data_hash) VALUES (?, ?, ?, ?, ?)",
                                (
                                    timestamp,
                                    int(row['Glucose Value (mg/dL)']) if pd.notna(row.get('Glucose Value (mg/dL)')) else None,
                                    float(row.get('Rate of Change', 0)) if pd.notna(row.get('Rate of Change')) else None,
                                    str(row.get('Transmitter ID', '')) if pd.notna(row.get('Transmitter ID')) else None,
                                    row_hash
                                )
                            )
                            results["glucose_readings"] += 1
                        except sqlite3.IntegrityError:
                            results["duplicates_skipped"] += 1
                
                # Check for carbs
                if 'Carbs (grams)' in df.columns and pd.notna(row.get('Carbs (grams)')):
                    timestamp = parse_timestamp(row)
                    if timestamp and float(row['Carbs (grams)']) > 0:
                        conn.execute(
                            "INSERT INTO carbs (timestamp, carbs_grams, data_hash) VALUES (?, ?, ?)",
                            (timestamp, int(row['Carbs (grams)']), row_hash)
                        )
                        results["carbs"] += 1
                
                # Check for insulin
                if 'Insulin Value (u)' in df.columns and pd.notna(row.get('Insulin Value (u)')):
                    timestamp = parse_timestamp(row)
                    if timestamp and float(row['Insulin Value (u)']) > 0:
                        conn.execute(
                            "INSERT INTO insulin (timestamp, insulin_type, units, data_hash) VALUES (?, ?, ?, ?)",
                            (timestamp, row.get('Insulin Type', 'Unknown'), float(row['Insulin Value (u)']), row_hash)
                        )
                        results["insulin"] += 1
                        
            except Exception as e:
                results["errors"].append(str(e))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Import complete: {results}")
        return JSONResponse(results)
        
    except Exception as e:
        logger.error(f"Import error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


def find_header_row(content: str) -> int:
    """Find the row with column headers in Clarity export"""
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'Timestamp' in line or 'Glucose Value' in line:
            return i
    return 0


def parse_timestamp(row) -> Optional[str]:
    """Parse timestamp from various Clarity export formats"""
    # Try different column names
    for col in ['Timestamp (YYYY-MM-DDThh:mm:ss)', 'Timestamp', 'Event Time']:
        if col in row.index and pd.notna(row[col]):
            try:
                ts = pd.to_datetime(row[col])
                return ts.isoformat()
            except:
                pass
    return None


# ============ Health Check ============

@app.get("/")
@app.get("/health")
async def health():
    """Health check endpoint"""
    conn = get_db()
    try:
        count = conn.execute("SELECT COUNT(*) FROM glucose_readings").fetchone()[0]
        return {
            "status": "healthy",
            "version": MCP_VERSION,
            "glucose_readings_count": count
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
