# stelo-workaround-mcp

FastAPI service to import Dexcom Stelo/Clarity CSV exports into SQLite, with MCP endpoints for Simtheory.ai.

## Why?

Dexcom API v3 does not support Stelo device data. This workaround lets you:
1. Export CSV from Dexcom Clarity
2. Import into local SQLite database
3. Query via MCP endpoints

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/import` | Upload Clarity CSV file |
| GET | `/glucose/latest?hours=24` | Last N hours of readings |
| GET | `/glucose/range?start=&end=` | Date range query |
| GET | `/glucose/stats?days=14` | TIR, avg, GMI, CV stats |
| GET | `/mcp` | MCP manifest for Simtheory.ai |
| GET | `/health` | Health check |

## Deployment

### Docker (Coolify)

```bash
docker build -t stelo-workaround-mcp .
docker run -d -p 8085:8085 -v ~/data:/data stelo-workaround-mcp
```

### Manual

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8085
```

## Usage

```bash
# Import CSV
curl -X POST -F "file=@clarity_export.csv" http://timserver:8085/import

# Get stats
curl http://timserver:8085/glucose/stats

# Last 24h
curl http://timserver:8085/glucose/latest
```

## Schema Migrations

Auto-migrations run on startup. To add a new migration:
1. Increment `LATEST_SCHEMA_VERSION`
2. Add migration function `_apply_migration_vN()`
3. Register in `MIGRATIONS` dict

## License

MIT
