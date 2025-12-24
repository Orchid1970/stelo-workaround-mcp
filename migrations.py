"""Database migrations for stelo-workaround-mcp"""
import sqlite3
import logging

logger = logging.getLogger(__name__)

MIGRATIONS = [
    {
        "version": 1,
        "description": "Add data_hash column to glucose_readings",
        "sql": [
            "ALTER TABLE glucose_readings ADD COLUMN data_hash TEXT;"
        ]
    },
    {
        "version": 2,
        "description": "Add transmitter_id to support multiple sensors (Stelo sensors change every 14-15 days)",
        "sql": [
            "ALTER TABLE glucose_readings ADD COLUMN transmitter_id TEXT;",
            "CREATE INDEX IF NOT EXISTS idx_transmitter ON glucose_readings(transmitter_id);",
            "CREATE INDEX IF NOT EXISTS idx_timestamp_transmitter ON glucose_readings(timestamp, transmitter_id);"
        ]
    },
    {
        "version": 3,
        "description": "Clean up transmitter_id values - remove .0 suffix from float conversion",
        "sql": [
            "UPDATE glucose_readings SET transmitter_id = REPLACE(transmitter_id, '.0', '') WHERE transmitter_id LIKE '%.0';"
        ]
    },
    {
        "version": 4,
        "description": "Remove UNIQUE constraint on (timestamp, transmitter_id) - SQLite requires table rebuild",
        "sql": [
            # SQLite doesn't support DROP CONSTRAINT, need to rebuild table
            """CREATE TABLE glucose_readings_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                glucose_value INTEGER NOT NULL,
                rate_of_change REAL,
                transmitter_id TEXT,
                data_hash TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""",
            "INSERT INTO glucose_readings_new SELECT id, timestamp, glucose_value, rate_of_change, transmitter_id, data_hash, created_at FROM glucose_readings",
            "DROP TABLE glucose_readings",
            "ALTER TABLE glucose_readings_new RENAME TO glucose_readings",
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON glucose_readings(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_transmitter ON glucose_readings(transmitter_id)",
            "CREATE INDEX IF NOT EXISTS idx_timestamp_transmitter ON glucose_readings(timestamp, transmitter_id)"
        ]
    }
]

def get_schema_version(conn: sqlite3.Connection) -> int:
    """Get current schema version from database"""
    try:
        cursor = conn.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
        row = cursor.fetchone()
        return row[0] if row else 0
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        return 0

def ensure_schema_version_table(conn: sqlite3.Connection):
    """Create schema_version table if it doesn't exist"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            description TEXT
        )
    """)
    conn.commit()

def run_migrations(db_path: str):
    """Run all pending migrations"""
    conn = sqlite3.connect(db_path)
    
    try:
        ensure_schema_version_table(conn)
        current_version = get_schema_version(conn)
        
        for migration in MIGRATIONS:
            if migration["version"] > current_version:
                logger.info(f"Running migration {migration['version']}: {migration['description']}")
                
                for sql in migration["sql"]:
                    try:
                        conn.execute(sql)
                        logger.info(f"  Executed: {sql[:80]}...")
                    except sqlite3.OperationalError as e:
                        # Column might already exist
                        if "duplicate column" in str(e).lower():
                            logger.info(f"  Column already exists, skipping")
                        else:
                            raise
                
                conn.execute(
                    "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                    (migration["version"], migration["description"])
                )
                conn.commit()
                logger.info(f"Migration {migration['version']} complete")
        
        logger.info(f"Database schema is up to date (version {get_schema_version(conn)})")
    
    finally:
        conn.close()

def check_and_add_data_hash_column(db_path: str):
    """Directly check and add data_hash column if missing (fallback method)"""
    conn = sqlite3.connect(db_path)
    try:
        # Check if column exists
        cursor = conn.execute("PRAGMA table_info(glucose_readings)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if "data_hash" not in columns:
            logger.info("Adding missing data_hash column to glucose_readings")
            conn.execute("ALTER TABLE glucose_readings ADD COLUMN data_hash TEXT")
            conn.commit()
            logger.info("data_hash column added successfully")
        else:
            logger.info("data_hash column already exists")
    finally:
        conn.close()
