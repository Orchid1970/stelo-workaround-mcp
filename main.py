"""
Stelo Glucose MCP Server - Workaround for Dexcom Stelo
Uploads Dexcom Clarity CSV exports and provides glucose data via MCP tools.
Version: 2.3.0 - Add HTTP POST JSON-RPC handler for Simtheory compatibility
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

logger.info(f"Starting Stelo MCP v2.3.0")
logger.info(f"Database path: {DB_PATH}")

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize FastMCP
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


# ============== MCP Tools ==============

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


# ============== FastAPI App ==============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    logger.info("Application starting up...")
    yield
    logger.info("Application shutting down...")


# Create FastAPI app
app = FastAPI(title="Stelo Glucose MCP", version="2.3.0", lifespan=lifespan)


# ============== MCP JSON-RPC Handler (for Simtheory compatibility) ==============

# Tool definitions for tools/list response
MCP_TOOLS = [
    {
        "name": "get_latest_glucose",
        "description": "Get the most recent glucose readings",
        "inputSchema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 24, "description": "Number of hours to look back"},
                "limit": {"type": "integer", "default": 50, "description": "Maximum number of readings to return"}
            }
        }
    },
    {
        "name": "get_glucose_stats",
        "description": "Get glucose statistics for the specified number of days including average, min, max, std dev, time in range",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 7, "description": "Number of days to analyze"}
            }
        }
    },
    {
        "name": "get_summary",
        "description": "Get a comprehensive summary of glucose data and logged entries",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 14, "description": "Number of days to summarize"}
            }
        }
    },
    {
        "name": "get_glucose_by_date",
        "description": "Get all glucose readings for a specific date",
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
                "insulin_type": {"type": "string", "default": "rapid", "description": "Type of insulin (rapid, long, mixed)"},
                "notes": {"type": "string", "default": "", "description": "Optional notes about the dose"}
            },
            "required": ["units"]
        }
    }
]


@app.post("/")
async def mcp_jsonrpc_root(request: Request):
    """Handle MCP JSON-RPC requests at root endpoint (Simtheory compatibility)."""
    return await handle_mcp_jsonrpc(request)


@app.post("/mcp")
async def mcp_jsonrpc_mcp(request: Request):
    """Handle MCP JSON-RPC requests at /mcp endpoint."""
    return await handle_mcp_jsonrpc(request)


async def handle_mcp_jsonrpc(request: Request):
    """Process MCP JSON-RPC requests."""
    try:
        body = await request.json()
        method = body.get("method", "")
        request_id = body.get("id", 1)
        params = body.get("params", {})
        
        logger.info(f"MCP JSON-RPC request: method={method}, id={request_id}")
        
        # Handle initialize
        if method == "initialize":
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "stelo-glucose-mcp", "version": "2.3.0"}
                }
            })
        
        # Handle tools/list
        elif method == "tools/list":
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"tools": MCP_TOOLS}
            })
        
        # Handle tools/call
        elif method == "tools/call":
            tool_name = params.get("name", "")
            arguments = params.get("arguments", {})
            
            logger.info(f"Calling tool: {tool_name} with args: {arguments}")
            
            # Route to appropriate tool
            if tool_name == "get_latest_glucose":
                result = await get_latest_glucose(
                    hours=arguments.get("hours", 24),
                    limit=arguments.get("limit", 50)
                )
            elif tool_name == "get_glucose_stats":
                result = await get_glucose_stats(
                    days=arguments.get("days", 7)
                )
            elif tool_name == "get_summary":
                result = await get_summary(
                    days=arguments.get("days", 14)
                )
            elif tool_name == "get_glucose_by_date":
                date = arguments.get("date")
                if not date:
                    return JSONResponse({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32602, "message": "Missing required parameter: date"}
                    })
                result = await get_glucose_by_date(date=date)
            elif tool_name == "log_insulin":
                units = arguments.get("units")
                if units is None:
                    return JSONResponse({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -32602, "message": "Missing required parameter: units"}
                    })
                result = await log_insulin(
                    units=units,
                    insulin_type=arguments.get("insulin_type", "rapid"),
                    notes=arguments.get("notes", "")
                )
            else:
                return JSONResponse({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}
                })
            
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"content": [{"type": "text", "text": result}]}
            })
        
        # Handle notifications/initialized (no response needed but acknowledge)
        elif method == "notifications/initialized":
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {}
            })
        
        # Unknown method
        else:
            logger.warning(f"Unknown MCP method: {method}")
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"}
            })
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": -32700, "message": "Parse error"}
        })
    except Exception as e:
        logger.error(f"MCP JSON-RPC error: {e}")
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": 1,
            "error": {"code": -32603, "message": str(e)}
        })


# ============== REST Endpoints ==============

@app.get("/")
async def root():
    """Root endpoint - service info."""
    return {
        "service": "stelo-glucose-mcp",
        "version": "2.3.0",
        "protocol": "MCP JSON-RPC",
        "description": "Timothy's Stelo glucose data integration for Simtheory.ai",
        "available_tools": [t["name"] for t in MCP_TOOLS],
        "endpoints": {
            "mcp_protocol": "POST /",
            "mcp_alt": "POST /mcp",
            "health": "GET /health",
            "upload": "POST /upload/clarity"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        await ensure_db_ready()
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM glucose_readings")
            count = (await cursor.fetchone())[0]
        return {"status": "healthy", "version": "2.3.0", "glucose_readings_count": count}
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
