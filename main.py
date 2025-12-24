"""
Stelo Glucose MCP Server - Workaround for Dexcom Stelo
Uploads Dexcom Clarity CSV exports and provides glucose data via MCP tools.
Version: 2.3.2 - Use correct table/column names matching existing database
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import aiosqlite
from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database path - use /data for Railway volume persistence
DB_PATH = os.environ.get("DB_PATH", "/data/stelo.db")

logger.info(f"Starting Stelo MCP v2.3.2")
logger.info(f"Database path: {DB_PATH}")

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize FastMCP (keeping for potential future SSE support)
mcp = FastMCP("Stelo Glucose")

# Flag to track if migrations have run
_migrations_done = False

# Cached glucose column name (detected at runtime)
_glucose_column = None

async def ensure_db_ready():
    """Ensure database is initialized. Called lazily on first request."""
    global _migrations_done
    if not _migrations_done:
        from migrations import run_migrations
        run_migrations(DB_PATH)
        logger.info("Database initialized and migrations complete")
        _migrations_done = True

async def get_glucose_column() -> str:
    """Detect the glucose column name in the database."""
    global _glucose_column
    if _glucose_column is not None:
        return _glucose_column
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("PRAGMA table_info(glucose_readings)")
        columns = [row[1] for row in await cursor.fetchall()]
        logger.info(f"Detected columns in glucose_readings: {columns}")
        
        # Check for known column names in order of preference
        if 'glucose_value' in columns:
            _glucose_column = 'glucose_value'
        elif 'glucose_mg_dl' in columns:
            _glucose_column = 'glucose_mg_dl'
        elif 'value' in columns:
            _glucose_column = 'value'
        else:
            # Default, might fail but let's try
            _glucose_column = 'glucose_value'
        
        logger.info(f"Using glucose column: {_glucose_column}")
        return _glucose_column


# ============== MCP Tool Definitions (for JSON-RPC tools/list) ==============

MCP_TOOLS = [
    {
        "name": "get_latest_glucose",
        "description": "Get the most recent glucose readings from Dexcom Stelo",
        "inputSchema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 24, "description": "Number of hours to look back (default 24)"},
                "limit": {"type": "integer", "default": 50, "description": "Maximum number of readings to return (default 50)"}
            }
        }
    },
    {
        "name": "get_glucose_stats",
        "description": "Get glucose statistics for the specified number of days, including average, min, max, standard deviation, and time in range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 7, "description": "Number of days to analyze (default 7)"}
            }
        }
    },
    {
        "name": "get_summary",
        "description": "Get a comprehensive summary of glucose data including stats, patterns, and logged insulin/carb entries",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 14, "description": "Number of days to summarize (default 14)"}
            }
        }
    },
    {
        "name": "get_glucose_by_date",
        "description": "Get glucose readings for a specific date",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "Date in YYYY-MM-DD format"}
            },
            "required": ["date"]
        }
    },
    {
        "name": "log_insulin",
        "description": "Log an insulin dose",
        "inputSchema": {
            "type": "object",
            "properties": {
                "units": {"type": "number", "description": "Number of insulin units"},
                "insulin_type": {"type": "string", "default": "rapid", "description": "Type of insulin (rapid, long, etc.)"},
                "notes": {"type": "string", "default": "", "description": "Optional notes"}
            },
            "required": ["units"]
        }
    }
]


# ============== MCP Tool Implementations ==============

async def get_latest_glucose(hours: int = 24, limit: int = 50) -> str:
    """Get the most recent glucose readings."""
    try:
        await ensure_db_ready()
        glucose_col = await get_glucose_column()
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                f"""SELECT timestamp, {glucose_col} as glucose_mg_dl 
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


async def get_glucose_stats(days: int = 7) -> str:
    """Get glucose statistics for the specified number of days."""
    try:
        await ensure_db_ready()
        glucose_col = await get_glucose_column()
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                f"""SELECT {glucose_col} as glucose_mg_dl FROM glucose_readings 
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


async def get_summary(days: int = 14) -> str:
    """Get a comprehensive summary of glucose data and logged entries."""
    try:
        await ensure_db_ready()
        glucose_col = await get_glucose_column()
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            
            # Get glucose readings
            cursor = await db.execute(
                f"""SELECT timestamp, {glucose_col} as glucose_mg_dl FROM glucose_readings 
                   WHERE timestamp >= ? ORDER BY timestamp""",
                (cutoff,)
            )
            glucose_rows = await cursor.fetchall()
            
            # Get insulin entries (table is 'insulin', not 'insulin_entries')
            cursor = await db.execute(
                """SELECT timestamp, units, insulin_type FROM insulin 
                   WHERE timestamp >= ? ORDER BY timestamp""",
                (cutoff,)
            )
            insulin_rows = await cursor.fetchall()
            
            # Get carb entries (table is 'carbs', column is 'carbs_grams')
            cursor = await db.execute(
                """SELECT timestamp, carbs_grams FROM carbs 
                   WHERE timestamp >= ? ORDER BY timestamp""",
                (cutoff,)
            )
            carb_rows = await cursor.fetchall()
            
            if not glucose_rows:
                return json.dumps({"error": "No glucose readings found for this period"})
            
            values = [row["glucose_mg_dl"] for row in glucose_rows]
            avg = sum(values) / len(values)
            in_range = sum(1 for v in values if 70 <= v <= 180)
            
            summary = {
                "period_days": days,
                "glucose": {
                    "total_readings": len(values),
                    "average_mg_dl": round(avg, 1),
                    "min_mg_dl": min(values),
                    "max_mg_dl": max(values),
                    "time_in_range_percent": round(in_range / len(values) * 100, 1)
                },
                "insulin_entries": len(insulin_rows),
                "carb_entries": len(carb_rows),
                "recent_insulin": [
                    {"timestamp": row["timestamp"], "units": row["units"], "type": row["insulin_type"]}
                    for row in insulin_rows[-5:]
                ],
                "recent_carbs": [
                    {"timestamp": row["timestamp"], "grams": row["carbs_grams"]}
                    for row in carb_rows[-5:]
                ]
            }
            return json.dumps(summary, indent=2)
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return json.dumps({"error": str(e)})


async def get_glucose_by_date(date: str) -> str:
    """Get glucose readings for a specific date."""
    try:
        await ensure_db_ready()
        glucose_col = await get_glucose_column()
        
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                f"""SELECT timestamp, {glucose_col} as glucose_mg_dl FROM glucose_readings 
                   WHERE timestamp LIKE ? ORDER BY timestamp""",
                (f"{date}%",)
            )
            rows = await cursor.fetchall()
            
            if not rows:
                return json.dumps({"error": f"No readings found for {date}"})
            
            readings = [{"timestamp": row["timestamp"], "glucose_mg_dl": row["glucose_mg_dl"]} for row in rows]
            values = [row["glucose_mg_dl"] for row in rows]
            
            return json.dumps({
                "date": date,
                "readings": readings,
                "count": len(readings),
                "average_mg_dl": round(sum(values) / len(values), 1),
                "min_mg_dl": min(values),
                "max_mg_dl": max(values)
            }, indent=2)
    except Exception as e:
        logger.error(f"Error fetching glucose by date: {e}")
        return json.dumps({"error": str(e)})


async def log_insulin(units: float, insulin_type: str = "rapid", notes: str = "") -> str:
    """Log an insulin dose."""
    try:
        await ensure_db_ready()
        timestamp = datetime.now().isoformat()
        data_hash = hashlib.sha256(f"{timestamp}:{units}:{insulin_type}".encode()).hexdigest()[:16]
        
        async with aiosqlite.connect(DB_PATH) as db:
            # Use 'insulin' table with correct columns
            await db.execute(
                """INSERT INTO insulin (timestamp, units, insulin_type, data_hash)
                   VALUES (?, ?, ?, ?)""",
                (timestamp, units, insulin_type, data_hash)
            )
            await db.commit()
        
        return json.dumps({
            "status": "success",
            "logged": {
                "timestamp": timestamp,
                "units": units,
                "insulin_type": insulin_type,
                "notes": notes
            }
        }, indent=2)
    except Exception as e:
        logger.error(f"Error logging insulin: {e}")
        return json.dumps({"error": str(e)})


# ============== FastAPI App Setup ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    logger.info("Application starting up...")
    yield
    logger.info("Application shutting down...")


app = FastAPI(
    title="Stelo Glucose MCP",
    description="Timothy's Dexcom Stelo glucose data integration for Simtheory.ai",
    version="2.3.2",
    lifespan=lifespan
)


# ============== MCP JSON-RPC Handler ==============

async def handle_mcp_jsonrpc(body: dict) -> dict:
    """Handle MCP JSON-RPC requests."""
    method = body.get("method", "")
    request_id = body.get("id", 1)
    params = body.get("params", {})
    
    logger.info(f"MCP JSON-RPC: method={method}, id={request_id}")
    
    # Handle initialize
    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "stelo-glucose-mcp", "version": "2.3.2"}
            }
        }
    
    # Handle notifications/initialized (acknowledgment, no response needed but we'll confirm)
    elif method == "notifications/initialized":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {}
        }
    
    # Handle tools/list
    elif method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {"tools": MCP_TOOLS}
        }
    
    # Handle tools/call
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        
        logger.info(f"Tool call: {tool_name} with args: {arguments}")
        
        try:
            if tool_name == "get_latest_glucose":
                result = await get_latest_glucose(**arguments)
            elif tool_name == "get_glucose_stats":
                result = await get_glucose_stats(**arguments)
            elif tool_name == "get_summary":
                result = await get_summary(**arguments)
            elif tool_name == "get_glucose_by_date":
                result = await get_glucose_by_date(**arguments)
            elif tool_name == "log_insulin":
                result = await log_insulin(**arguments)
            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}
                }
            
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"content": [{"type": "text", "text": result}]}
            }
        except Exception as e:
            logger.error(f"Tool execution error: {e}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(e)}
            }
    
    # Unknown method
    else:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }


# ============== HTTP Endpoints ==============

@app.get("/")
async def root():
    """Root endpoint - service info."""
    return {
        "service": "stelo-glucose-mcp",
        "version": "2.3.2",
        "protocol": "MCP JSON-RPC",
        "description": "Timothy's Dexcom Stelo glucose data integration for Simtheory.ai",
        "available_tools": [tool["name"] for tool in MCP_TOOLS],
        "endpoints": {
            "mcp_protocol": "POST /",
            "mcp_alt": "POST /mcp",
            "health": "GET /health",
            "upload": "POST /upload/clarity",
            "schema": "GET /debug/schema"
        }
    }


@app.post("/")
async def mcp_root(request: Request):
    """Handle MCP JSON-RPC requests at root (Simtheory.ai compatible)."""
    try:
        body = await request.json()
        response = await handle_mcp_jsonrpc(body)
        return JSONResponse(response)
    except Exception as e:
        logger.error(f"MCP JSON-RPC error at /: {e}")
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32603, "message": str(e)}
        })


@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """Handle MCP JSON-RPC requests at /mcp (alternative endpoint)."""
    try:
        body = await request.json()
        response = await handle_mcp_jsonrpc(body)
        return JSONResponse(response)
    except Exception as e:
        logger.error(f"MCP JSON-RPC error at /mcp: {e}")
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32603, "message": str(e)}
        })


@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        await ensure_db_ready()
        glucose_col = await get_glucose_column()
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM glucose_readings")
            count = (await cursor.fetchone())[0]
        return {
            "status": "healthy",
            "version": "2.3.2",
            "database": DB_PATH,
            "glucose_readings": count,
            "glucose_column": glucose_col
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/debug/schema")
async def debug_schema():
    """Debug endpoint to show database schema."""
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            # Get all tables
            cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in await cursor.fetchall()]
            
            schema = {}
            for table in tables:
                cursor = await db.execute(f"PRAGMA table_info({table})")
                columns = [{"name": row[1], "type": row[2]} for row in await cursor.fetchall()]
                schema[table] = columns
            
            return {
                "database": DB_PATH,
                "tables": tables,
                "schema": schema
            }
    except Exception as e:
        return {"error": str(e)}


@app.post("/upload/clarity")
async def upload_clarity_csv(file: UploadFile = File(...)):
    """Upload and process a Dexcom Clarity CSV export."""
    try:
        await ensure_db_ready()
        glucose_col = await get_glucose_column()
        content = await file.read()
        file_hash = hashlib.sha256(content).hexdigest()[:16]
        
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            text = content.decode('latin-1')
        
        lines = text.strip().split('\n')
        header_idx = 0
        for i, line in enumerate(lines):
            if 'Timestamp' in line or 'timestamp' in line:
                header_idx = i
                break
        
        from io import StringIO
        csv_data = '\n'.join(lines[header_idx:])
        df = pd.read_csv(StringIO(csv_data))
        df.columns = df.columns.str.strip()
        
        ts_col = None
        csv_glucose_col = None
        insulin_col = None
        carbs_col = None
        event_subtype_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'timestamp' in col_lower and ts_col is None:
                ts_col = col
            if 'glucose' in col_lower and 'mg' in col_lower:
                csv_glucose_col = col
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
                
                # Insert glucose readings
                if csv_glucose_col and pd.notna(row.get(csv_glucose_col)):
                    glucose_val = row[csv_glucose_col]
                    if isinstance(glucose_val, (int, float)) and glucose_val > 0:
                        reading_hash = hashlib.sha256(f"{timestamp}:{glucose_val}:{file_hash}".encode()).hexdigest()[:16]
                        cursor = await db.execute(
                            f"SELECT 1 FROM glucose_readings WHERE timestamp = ? AND {glucose_col} = ?",
                            (timestamp, int(glucose_val))
                        )
                        exists = await cursor.fetchone()
                        if not exists:
                            await db.execute(
                                f"""INSERT INTO glucose_readings (timestamp, {glucose_col}, data_hash)
                                   VALUES (?, ?, ?)""",
                                (timestamp, int(glucose_val), reading_hash)
                            )
                            inserted_glucose += 1
                        else:
                            skipped_duplicates += 1
                
                # Insert insulin entries (using 'insulin' table)
                if insulin_col and pd.notna(row.get(insulin_col)):
                    insulin_val = row[insulin_col]
                    if isinstance(insulin_val, (int, float)) and insulin_val > 0:
                        cursor = await db.execute(
                            "SELECT 1 FROM insulin WHERE timestamp = ? AND units = ?",
                            (timestamp, float(insulin_val))
                        )
                        exists = await cursor.fetchone()
                        if not exists:
                            insulin_type = "rapid"
                            if event_subtype_col and pd.notna(row.get(event_subtype_col)):
                                subtype = str(row[event_subtype_col]).lower()
                                if 'long' in subtype:
                                    insulin_type = "long"
                            ins_hash = hashlib.sha256(f"{timestamp}:{insulin_val}:{file_hash}".encode()).hexdigest()[:16]
                            await db.execute(
                                """INSERT INTO insulin (timestamp, units, insulin_type, data_hash)
                                   VALUES (?, ?, ?, ?)""",
                                (timestamp, float(insulin_val), insulin_type, ins_hash)
                            )
                            inserted_insulin += 1
                
                # Insert carb entries (using 'carbs' table with 'carbs_grams' column)
                if carbs_col and pd.notna(row.get(carbs_col)):
                    carbs_val = row[carbs_col]
                    if isinstance(carbs_val, (int, float)) and carbs_val > 0:
                        cursor = await db.execute(
                            "SELECT 1 FROM carbs WHERE timestamp = ? AND carbs_grams = ?",
                            (timestamp, int(carbs_val))
                        )
                        exists = await cursor.fetchone()
                        if not exists:
                            carb_hash = hashlib.sha256(f"{timestamp}:{carbs_val}:{file_hash}".encode()).hexdigest()[:16]
                            await db.execute(
                                """INSERT INTO carbs (timestamp, carbs_grams, data_hash)
                                   VALUES (?, ?, ?)""",
                                (timestamp, int(carbs_val), carb_hash)
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
