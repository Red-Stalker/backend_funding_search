import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Database — new v2 DB with 5-min data (old funding.db is preserved)
DB_PATH = os.environ.get("FUNDING_DB_PATH", str(BASE_DIR / "funding_v2.db"))

# How many days back to fetch on backfill
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "60"))

# Collector fetches last N hours of data to catch any missed settlements
COLLECTOR_LOOKBACK_HOURS = int(os.environ.get("COLLECTOR_LOOKBACK_HOURS", "24"))

# Rate limit delay between per-symbol requests (seconds)
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "0.15"))

# Max concurrent exchange fetches during backfill
BACKFILL_CONCURRENCY = int(os.environ.get("BACKFILL_CONCURRENCY", "3"))

# Snapshot collection interval in minutes (predicted rates → current_rates only)
SNAPSHOT_INTERVAL_MINUTES = int(os.environ.get("SNAPSHOT_INTERVAL_MINUTES", "60"))

# Max concurrent per-symbol requests during snapshot collection
SNAPSHOT_CONCURRENCY = int(os.environ.get("SNAPSHOT_CONCURRENCY", "10"))

# Settlement collection — fetches actual settlement rates into funding_rates
SETTLEMENT_INTERVAL_MINUTES = int(os.environ.get("SETTLEMENT_INTERVAL_MINUTES", "60"))
SETTLEMENT_CONCURRENCY = int(os.environ.get("SETTLEMENT_CONCURRENCY", "5"))
SETTLEMENT_LOOKBACK_HOURS = int(os.environ.get("SETTLEMENT_LOOKBACK_HOURS", "12"))

# Integrity check on startup — fill gaps from loris.tools
INTEGRITY_CHECK_ENABLED = os.environ.get("INTEGRITY_CHECK", "true").lower() == "true"
INTEGRITY_GAP_MINUTES = int(os.environ.get("INTEGRITY_GAP_MINUTES", "15"))

# Deep integrity check — periodic per-exchange gap detection + loris fill
DEEP_INTEGRITY_ENABLED = os.environ.get("DEEP_INTEGRITY", "true").lower() == "true"
DEEP_INTEGRITY_INTERVAL_HOURS = int(os.environ.get("DEEP_INTEGRITY_HOURS", "6"))
DEEP_INTEGRITY_WINDOW_DAYS = int(os.environ.get("DEEP_INTEGRITY_DAYS", "7"))

# Proxy file for loris.tools requests
PROXY_FILE = os.environ.get("PROXY_FILE", str(BASE_DIR / "proxies.txt"))

# Server
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8000"))
