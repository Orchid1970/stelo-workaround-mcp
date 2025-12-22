"""
Stelo Workaround MCP - v2.1.0
Dexcom Stelo glucose data via Clarity CSV import
Now with proper MCP JSON-RPC protocol support
"""

import os
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta
from typing import Optional
from contextlib import contextmanager

from fastapi import FastAPI, File, UploadFile, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn
import csv
import io

# Configuration
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.path.join(DATA_DIR, "glucose.db")

app = FastAPI(
    title="Stelo Workaround MCP",
    version="2.1.0",
    description="Dexcom Stelo glucose data via Clarity CSV import with MCP JSON-RPC protocol"
)

# ============== Database Setup ==============

def init_db():
    """Initialize SQLite database with all tables"""
    os.makedirs(DATA_DIR, exist_ok=True)
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Glucose readings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS glucose_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                glucose_value INTEGER NOT NULL,
                rate_of_change REAL,
                transmitter_id TEXT,
                data_hash TEXT UNIQUE
            )
        """)
        
        # Activities table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                activity_type TEXT,
                duration_minutes INTEGER,
                data_hash TEXT UNIQUE
            )
        """)
        
        # Alerts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                alert_type TEXT,
                message TEXT,
                data_hash TEXT UNIQUE
            )
        """)
        
        # Carbs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS carbs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                carb_grams INTEGER,
                data_hash TEXT UNIQUE
            )
        """)
        
        # Insulin table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS insulin (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                insulin_units REAL,
                insulin_type TEXT,
                data_hash TEXT UNIQUE
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_glucose_timestamp ON glucose_readings(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_activities_timestamp ON activities(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp)")
        
        conn.commit()

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
init_db()

# ============== CSV Import Logic ==============

def compute_hash(data: dict) -> str:
    """Compute a hash for deduplication"""
    hash_str = json.dumps(data, sort_keys=True)
    return hashlib.md5(hash_str.encode()).hexdigest()

def parse_dexcom_timestamp(ts: str) -> Optional[str]:
    """Parse various Dexcom timestamp formats"""
    if not ts or ts.strip() == "":
        return None
    
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M %p",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(ts.strip(), fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None

def import_csv_data(content: str) -> dict:
    """Import Dexcom Clarity CSV data"""
    results = {
        "glucose_readings": 0,
        "activities": 0,
        "alerts": 0,
        "carbs": 0,
        "insulin": 0,
        "duplicates_skipped": 0,
        "errors": []
    }
    
    reader = csv.DictReader(io.StringIO(content))
    
    with get_db() as conn:
        cursor = conn.cursor()
        
        for row in reader:
            try:
                # Try to get timestamp from various possible column names
                timestamp = None
                for col in ["Timestamp (YYYY-MM-DDThh:mm:ss)", "Timestamp", "timestamp", "Time", "Date"]:
                    if col in row and row[col]:
                        timestamp = parse_dexcom_timestamp(row[col])
                        if timestamp:
                            break
                
                if not timestamp:
                    continue
                
                # Check for glucose reading
                glucose_value = None
                for col in ["Glucose Value (mg/dL)", "Glucose Value", "glucose_value", "CGM Glucose Value (mg/dL)"]:
                    if col in row and row[col]:
                        try:
                            val = row[col].strip()
                            if val and val.lower() not in ["low", "high", ""]:
                                glucose_value = int(float(val))
                                break
                        except (ValueError, AttributeError):
                            continue
                
                if glucose_value:
                    transmitter_id = row.get("Transmitter ID", row.get("transmitter_id", ""))
                    data_hash = compute_hash({"timestamp": timestamp, "glucose": glucose_value, "tx": transmitter_id})
                    
                    try:
                        cursor.execute(
                            "INSERT INTO glucose_readings (timestamp, glucose_value, transmitter_id, data_hash) VALUES (?, ?, ?, ?)",
                            (timestamp, glucose_value, transmitter_id, data_hash)
                        )
                        results["glucose_readings"] += 1
                    except sqlite3.IntegrityError:
                        results["duplicates_skipped"] += 1
                
                # Check for activity/exercise
                activity_type = row.get("Event Type", row.get("activity_type", ""))
                if activity_type and "exercise" in activity_type.lower():
                    duration = row.get("Duration (minutes)", row.get("duration", 0))
                    try:
                        duration = int(float(duration)) if duration else 0
                    except ValueError:
                        duration = 0
                    
                    data_hash = compute_hash({"timestamp": timestamp, "activity": activity_type})
                    
                    try:
                        cursor.execute(
                            "INSERT INTO activities (timestamp, activity_type, duration_minutes, data_hash) VALUES (?, ?, ?, ?)",
                            (timestamp, activity_type, duration, data_hash)
                        )
                        results["activities"] += 1
                    except sqlite3.IntegrityError:
                        results["duplicates_skipped"] += 1
                
                # Check for carbs
                carbs = row.get("Carbs (grams)", row.get("carbs", ""))
                if carbs:
                    try:
                        carb_grams = int(float(carbs))
                        if carb_grams > 0:
                            data_hash = compute_hash({"timestamp": timestamp, "carbs": carb_grams})
                            try:
                                cursor.execute(
                                    "INSERT INTO carbs (timestamp, carb_grams, data_hash) VALUES (?, ?, ?)",
                                    (timestamp, carb_grams, data_hash)
                                )
                                results["carbs"] += 1
                            except sqlite3.IntegrityError:
                                results["duplicates_skipped"] += 1
                    except ValueError:
                        pass
                
                # Check for insulin
                insulin = row.get("Insulin (units)", row.get("insulin", ""))
                if insulin:
                    try:
                        insulin_units = float(insulin)
                        if insulin_units > 0:
                            insulin_type = row.get("Insulin Type", "unknown")
                            data_hash = compute_hash({"timestamp": timestamp, "insulin": insulin_units})
                            try:
                                cursor.execute(
                                    "INSERT INTO insulin (timestamp, insulin_units, insulin_type, data_hash) VALUES (?, ?, ?, ?)",
                                    (timestamp, insulin_units, insulin_type, data_hash)
                                )
                                results["insulin"] += 1
                            except sqlite3.IntegrityError:
                                results["duplicates_skipped"] += 1
                    except ValueError:
                        pass
                        
            except Exception as e:
                results["errors"].append(str(e))
        
        conn.commit()
    
    return results

# ============== MCP JSON-RPC Protocol ==============

MCP_TOOLS = [
    {
        "name": "get_latest_glucose",
        "description": "Get the most recent glucose readings. Returns timestamps and glucose values in mg/dL.",
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
        "name": "get_glucose_range",
        "description": "Get glucose readings within a specific date range.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date in YYYY-MM-DD format"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date in YYYY-MM-DD format"
                }
            },
            "required": ["start_date", "end_date"]
        }
    },
    {
        "name": "get_glucose_stats",
        "description": "Get glucose statistics including average, min, max, time in range, and variability metrics.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of days to analyze (default: 14)",
                    "default": 14
                }
            }
        }
    },
    {
        "name": "get_latest_activities",
        "description": "Get recent exercise and activity entries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of activities to return (default: 20)",
                    "default": 20
                }
            }
        }
    },
    {
        "name": "get_latest_alerts",
        "description": "Get recent glucose alerts (high/low warnings).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of alerts to return (default: 20)",
                    "default": 20
                }
            }
        }
    },
    {
        "name": "get_summary",
        "description": "Get a comprehensive health summary including glucose stats, recent activities, and alerts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of days to summarize (default: 14)",
                    "default": 14
                }
            }
        }
    }
]

def execute_tool(tool_name: str, arguments: dict) -> dict:
    """Execute an MCP tool and return the result"""
    
    if tool_name == "get_latest_glucose":
        hours = arguments.get("hours", 24)
        limit = arguments.get("limit", 100)
        since = datetime.now() - timedelta(hours=hours)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT timestamp, glucose_value, rate_of_change, transmitter_id 
                   FROM glucose_readings 
                   WHERE timestamp >= ? 
                   ORDER BY timestamp DESC 
                   LIMIT ?""",
                (since.strftime("%Y-%m-%dT%H:%M:%S"), limit)
            )
            readings = [dict(row) for row in cursor.fetchall()]
        
        return {"count": len(readings), "data": readings}
    
    elif tool_name == "get_glucose_range":
        start_date = arguments.get("start_date")
        end_date = arguments.get("end_date")
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT timestamp, glucose_value, rate_of_change, transmitter_id 
                   FROM glucose_readings 
                   WHERE timestamp >= ? AND timestamp <= ?
                   ORDER BY timestamp DESC""",
                (f"{start_date}T00:00:00", f"{end_date}T23:59:59")
            )
            readings = [dict(row) for row in cursor.fetchall()]
        
        return {"count": len(readings), "start_date": start_date, "end_date": end_date, "data": readings}
    
    elif tool_name == "get_glucose_stats":
        days = arguments.get("days", 14)
        since = datetime.now() - timedelta(days=days)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT glucose_value FROM glucose_readings WHERE timestamp >= ?""",
                (since.strftime("%Y-%m-%dT%H:%M:%S"),)
            )
            values = [row[0] for row in cursor.fetchall()]
        
        if not values:
            return {"error": "No glucose data found for the specified period"}
        
        avg_glucose = sum(values) / len(values)
        min_glucose = min(values)
        max_glucose = max(values)
        
        # Time in range (70-180 mg/dL)
        in_range = sum(1 for v in values if 70 <= v <= 180)
        below_range = sum(1 for v in values if v < 70)
        above_range = sum(1 for v in values if v > 180)
        
        # Glucose variability (standard deviation)
        variance = sum((v - avg_glucose) ** 2 for v in values) / len(values)
        std_dev = variance ** 0.5
        cv = (std_dev / avg_glucose) * 100  # Coefficient of variation
        
        return {
            "days_analyzed": days,
            "total_readings": len(values),
            "average_glucose": round(avg_glucose, 1),
            "min_glucose": min_glucose,
            "max_glucose": max_glucose,
            "std_deviation": round(std_dev, 1),
            "coefficient_of_variation": round(cv, 1),
            "time_in_range_percent": round((in_range / len(values)) * 100, 1),
            "time_below_range_percent": round((below_range / len(values)) * 100, 1),
            "time_above_range_percent": round((above_range / len(values)) * 100, 1)
        }
    
    elif tool_name == "get_latest_activities":
        limit = arguments.get("limit", 20)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT timestamp, activity_type, duration_minutes 
                   FROM activities 
                   ORDER BY timestamp DESC 
                   LIMIT ?""",
                (limit,)
            )
            activities = [dict(row) for row in cursor.fetchall()]
        
        return {"count": len(activities), "data": activities}
    
    elif tool_name == "get_latest_alerts":
        limit = arguments.get("limit", 20)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT timestamp, alert_type, message 
                   FROM alerts 
                   ORDER BY timestamp DESC 
                   LIMIT ?""",
                (limit,)
            )
            alerts = [dict(row) for row in cursor.fetchall()]
        
        return {"count": len(alerts), "data": alerts}
    
    elif tool_name == "get_summary":
        days = arguments.get("days", 14)
        
        # Get glucose stats
        stats = execute_tool("get_glucose_stats", {"days": days})
        
        # Get recent activities
        activities = execute_tool("get_latest_activities", {"limit": 10})
        
        # Get recent alerts
        alerts = execute_tool("get_latest_alerts", {"limit": 10})
        
        return {
            "period_days": days,
            "glucose_stats": stats,
            "recent_activities": activities,
            "recent_alerts": alerts
        }
    
    else:
        return {"error": f"Unknown tool: {tool_name}"}

def handle_jsonrpc(request_data: dict) -> dict:
    """Handle MCP JSON-RPC requests"""
    method = request_data.get("method", "")
    request_id = request_data.get("id")
    params = request_data.get("params", {})
    
    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "stelo-workaround",
                    "version": "2.1.0"
                }
            }
        }
    
    elif method == "notifications/initialized":
        # This is a notification, no response needed but we return empty for compatibility
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {}
        }
    
    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": MCP_TOOLS
            }
        }
    
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        
        try:
            result = execute_tool(tool_name, arguments)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, indent=2)
                        }
                    ]
                }
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }
    
    elif method == "ping":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {}
        }
    
    else:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Method not found: {method}"
            }
        }

# ============== HTTP Endpoints ==============

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "version": "2.1.0"}

@app.post("/import")
async def import_csv(file: UploadFile = File(...)):
    """Import a Dexcom Clarity CSV file"""
    content = await file.read()
    content_str = content.decode("utf-8")
    results = import_csv_data(content_str)
    return results

@app.get("/summary")
async def get_summary(days: int = Query(default=14)):
    """Get a summary of glucose data"""
    result = execute_tool("get_summary", {"days": days})
    return result

@app.get("/glucose/latest")
async def get_latest_glucose(hours: int = Query(default=24), limit: int = Query(default=100)):
    """Get latest glucose readings"""
    result = execute_tool("get_latest_glucose", {"hours": hours, "limit": limit})
    return result

@app.get("/glucose/range")
async def get_glucose_range(start_date: str = Query(...), end_date: str = Query(...)):
    """Get glucose readings in a date range"""
    result = execute_tool("get_glucose_range", {"start_date": start_date, "end_date": end_date})
    return result

@app.get("/glucose/stats")
async def get_glucose_stats(days: int = Query(default=14)):
    """Get glucose statistics"""
    result = execute_tool("get_glucose_stats", {"days": days})
    return result

@app.get("/activities/latest")
async def get_latest_activities(limit: int = Query(default=20)):
    """Get latest activities"""
    result = execute_tool("get_latest_activities", {"limit": limit})
    return result

@app.get("/alerts/latest")
async def get_latest_alerts(limit: int = Query(default=20)):
    """Get latest alerts"""
    result = execute_tool("get_latest_alerts", {"limit": limit})
    return result

@app.get("/carbs/latest")
async def get_latest_carbs(limit: int = Query(default=20)):
    """Get latest carb entries"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp, carb_grams FROM carbs ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        carbs = [dict(row) for row in cursor.fetchall()]
    return {"count": len(carbs), "data": carbs}

@app.get("/insulin/latest")
async def get_latest_insulin(limit: int = Query(default=20)):
    """Get latest insulin entries"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp, insulin_units, insulin_type FROM insulin ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        )
        insulin = [dict(row) for row in cursor.fetchall()]
    return {"count": len(insulin), "data": insulin}

# ============== MCP Endpoint ==============

@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """MCP JSON-RPC endpoint"""
    try:
        body = await request.json()
        response = handle_jsonrpc(body)
        return JSONResponse(content=response)
    except Exception as e:
        return JSONResponse(
            content={
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32700,
                    "message": f"Parse error: {str(e)}"
                }
            },
            status_code=400
        )

@app.get("/mcp")
async def mcp_info():
    """MCP info endpoint (for browser/testing)"""
    return {
        "name": "stelo-workaround",
        "version": "2.1.0",
        "protocol": "MCP JSON-RPC",
        "description": "Dexcom Stelo glucose data via Clarity CSV import",
        "tools": [t["name"] for t in MCP_TOOLS]
    }

# ============== Main ==============

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
