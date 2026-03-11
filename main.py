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
from collector import collect_snapshots, refresh_symbols
from exchanges.registry import close_all
from integrity import check_and_fill_gaps, fill_coverage_gaps

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
    """Background: check integrity and fill gaps, then run initial collection if needed."""
    try:
        await check_and_fill_gaps()
    except Exception as e:
        logger.error(f"Integrity check failed: {e}")

    # Fast per-symbol gap fill from loris (catches gaps missed by exchange-level check)
    try:
        await fill_coverage_gaps(window_days=7)
    except Exception as e:
        logger.error(f"Coverage gap fill failed: {e}")

    # Always refresh symbols so per-symbol exchanges (paradex, lighter, etc.) work
    try:
        logger.info("Refreshing exchange symbols...")
        await refresh_symbols()
        logger.info("Symbol refresh complete")
    except Exception as e:
        logger.error(f"Symbol refresh failed: {e}")

    # Run initial snapshot if needed
    try:
        current = await get_all_current_rates()
        if not current["symbols"] or len(current["symbols"]) < 10:
            logger.info("Running initial snapshot collection...")
            await collect_snapshots()
    except Exception as e:
        logger.error(f"Initial collection failed: {e}")


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

    return await get_historical_rates(symbol, exchange_list, start_ms, end_ms)


@app.get("/api/scan")
async def get_funding_scan(
    start: str = Query(..., description="Start time ISO 8601"),
    end: str = Query(..., description="End time ISO 8601"),
    exchanges: str = Query("", description="Comma-separated exchange names (empty = all)"),
):
    """Batch scan: compute funding spread for ALL symbols in one request.
    Returns {tickers: [{symbol, spread, minRate, maxRate, minEx, maxEx, rates, stability, smartScore}, ...]}
    """
    from datetime import datetime, timezone

    try:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        start_dt = datetime.fromtimestamp(float(start) / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(float(end) / 1000, tz=timezone.utc)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    if exchanges:
        exchange_list = [e.strip() for e in exchanges.split(",") if e.strip()]
    else:
        # All exchanges from current_rates
        current = await get_all_current_rates()
        exchange_list = list(current["funding_rates"].keys())

    # drift uses 1h interval, everything else 8h
    interval_map = {ex: 1 if ex == "drift" else 8 for ex in exchange_list}

    tickers = await get_bulk_scan(exchange_list, start_ms, end_ms, interval_map)
    return {"tickers": tickers}


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
