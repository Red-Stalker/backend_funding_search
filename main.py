import logging
import asyncio
import sys
from contextlib import asynccontextmanager

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from config import HOST, PORT, INTEGRITY_CHECK_ENABLED
from database import init_db, get_all_current_rates, get_historical_rates, get_bulk_scan, get_db_stats
from scheduler import start_scheduler, stop_scheduler
from collector import collect_snapshots, collect_settlements, refresh_symbols
from exchanges.registry import close_all
from integrity import fill_coverage_gaps

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    logger.info("Database initialized")

    # Start scheduler immediately so new data flows in
    start_scheduler()
    logger.info("Scheduler started")

    # Pre-compute scan cache so first requests are instant
    asyncio.create_task(_warmup_scan_cache())

    # Integrity check: fill data gaps from loris.tools (runs in background)
    if INTEGRITY_CHECK_ENABLED:
        asyncio.create_task(_startup_integrity())
    else:
        # If integrity check is disabled, still do initial collection if DB is empty
        current = await get_all_current_rates()
        if not current["symbols"]:
            logger.info("DB empty — running initial symbol refresh + collection")
            asyncio.create_task(_initial_load())

    yield

    # Shutdown
    stop_scheduler()
    await close_all()
    logger.info("Shutdown complete")


async def _startup_integrity():
    """Background: targeted gap fill + symbol refresh + initial snapshot."""
    from database import get_global_latest_ts
    import time

    # 1. Check data freshness (info only)
    latest_ts = await get_global_latest_ts()
    if latest_ts is None:
        logger.warning(
            "Database is EMPTY. Run 'python loris_download.py --days 60' first"
        )
    else:
        gap_hours = (int(time.time() * 1000) - latest_ts) / 3_600_000
        logger.info(f"Data freshness: {gap_hours:.1f}h since last record")

    # 2. Refresh symbols (needed for settlement collection)
    try:
        logger.info("Refreshing exchange symbols...")
        await refresh_symbols()
        logger.info("Symbol refresh complete")
    except Exception as e:
        logger.error(f"Symbol refresh failed: {e}")

    # 3. Targeted gap fill from loris (only symbols with < 85% coverage)
    try:
        await fill_coverage_gaps(window_days=7)
    except Exception as e:
        logger.error(f"Coverage gap fill failed: {e}")

    # 4. Initial snapshot if DB is nearly empty
    try:
        current = await get_all_current_rates()
        if not current["symbols"] or len(current["symbols"]) < 10:
            logger.info("Running initial snapshot collection...")
            await collect_snapshots()
    except Exception as e:
        logger.error(f"Initial collection failed: {e}")


async def _warmup_scan_cache():
    """Pre-compute scan results at startup so first user requests are instant."""
    try:
        from scan_cache import recompute_standard_scans
        await recompute_standard_scans()
    except Exception as e:
        logger.error(f"Scan cache warmup failed: {e}")


async def _initial_load():
    """Background task for first-time data loading."""
    try:
        await refresh_symbols()
        await collect_snapshots()
    except Exception as e:
        logger.error(f"Initial load failed: {e}")


app = FastAPI(title="Funding Rate Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/api/rates")
async def get_rates():
    """Returns all symbols and current funding rates per exchange.
    Response: {symbols: [...], funding_rates: {exchange: {symbol: rate}}}
    """
    return await get_all_current_rates()


@app.get("/api/funding/history")
async def get_funding_history(
    symbol: str = Query(..., description="Symbol e.g. BTC, ETH"),
    start: str = Query(..., description="Start time ISO 8601"),
    end: str = Query(..., description="End time ISO 8601"),
    exchanges: str = Query(..., description="Comma-separated exchange names"),
):
    """Returns historical funding rate series.
    Response: {series: {exchange: [{t: ISO, y: rate_bps}, ...]}}
    """
    from datetime import datetime, timezone

    # Parse ISO timestamps to milliseconds
    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        # Try parsing as timestamp
        start_dt = datetime.fromtimestamp(float(start) / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(float(end) / 1000, tz=timezone.utc)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    exchange_list = [e.strip() for e in exchanges.split(",") if e.strip()]

    # Build storage interval map: 8 for all normalized exchanges, native for drift
    from exchanges.registry import get_exchange as _get_ex
    interval_map = {}
    for ex_name in exchange_list:
        ex_inst = _get_ex(ex_name)
        if ex_inst and not ex_inst.normalize_to_8h:
            interval_map[ex_name] = ex_inst.native_interval_hours
        else:
            interval_map[ex_name] = 8

    return await get_historical_rates(symbol, exchange_list, start_ms, end_ms, interval_map)


@app.get("/api/scan")
async def get_funding_scan(
    start: str = Query(..., description="Start time ISO 8601"),
    end: str = Query(..., description="End time ISO 8601"),
    exchanges: str = Query("", description="Comma-separated exchange names (empty = all)"),
):
    """Batch scan: compute funding spread for ALL symbols in one request.
    Pre-computed for standard ranges (1d/7d/30d) — instant response.
    Custom ranges use on-demand cache (2min TTL).
    """
    from datetime import datetime, timezone
    from scan_cache import match_standard_range, get_precomputed, get_ondemand, set_ondemand

    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        start_dt = datetime.fromtimestamp(float(start) / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(float(end) / 1000, tz=timezone.utc)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    exchange_list = [e.strip() for e in exchanges.split(",") if e.strip()] if exchanges else None

    # Try pre-computed cache (works for ANY exchange combination, ~1ms)
    range_label = match_standard_range(start_ms, end_ms)
    if range_label:
        cached = get_precomputed(range_label, exchange_list)
        if cached:
            return cached

    # Resolve exchange list for on-demand computation
    if exchange_list is None:
        current = await get_all_current_rates()
        exchange_list = list(current["funding_rates"].keys())

    # On-demand cache for custom date ranges
    round_5min = 300_000
    cache_key = f"{sorted(exchange_list)}:{start_ms // round_5min}:{end_ms // round_5min}"

    cached = get_ondemand(cache_key)
    if cached:
        return cached

    from exchanges.registry import get_exchange as _get_ex
    interval_map = {}
    for ex_name in exchange_list:
        ex_inst = _get_ex(ex_name)
        if ex_inst and not ex_inst.normalize_to_8h:
            interval_map[ex_name] = ex_inst.native_interval_hours
        else:
            interval_map[ex_name] = 8
    tickers = await get_bulk_scan(exchange_list, start_ms, end_ms, interval_map)
    result = {"tickers": tickers}

    set_ondemand(cache_key, result)
    return result


@app.get("/api/status")
async def get_status():
    """Database status: record counts, date range, exchanges."""
    return await get_db_stats()


@app.get("/api/integrity")
async def get_integrity_status():
    """Returns the last deep integrity check report."""
    from integrity import get_last_report
    return get_last_report() or {"status": "no check run yet"}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=HOST, port=PORT, reload=False)
