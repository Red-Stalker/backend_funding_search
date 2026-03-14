import asyncio
import logging
import time

from config import REQUEST_DELAY, BACKFILL_CONCURRENCY, SETTLEMENT_CONCURRENCY, SETTLEMENT_LOOKBACK_HOURS, SETTLEMENT_DELAY
from database import upsert_funding_rates, upsert_current_rates, upsert_exchange_symbols, get_exchange_symbols, rebuild_current_rates
from exchanges.registry import get_all_exchanges, get_exchange

logger = logging.getLogger(__name__)


async def refresh_symbols(exchange_name: str | None = None):
    """Fetch and store symbol lists from exchange(s)."""
    exchanges = {exchange_name: get_exchange(exchange_name)} if exchange_name else get_all_exchanges()
    for name, ex in exchanges.items():
        if ex is None:
            continue
        try:
            symbols = await ex.fetch_symbols()
            if symbols:
                rows = [(name, sym, raw) for sym, raw in symbols]
                await upsert_exchange_symbols(rows)
                logger.info(f"[{name}] refreshed {len(symbols)} symbols")
            else:
                logger.warning(f"[{name}] no symbols returned")
        except Exception as e:
            logger.error(f"[{name}] symbol refresh failed: {e}")
        await asyncio.sleep(0.5)


async def collect_snapshots():
    """Snapshot job: fetch current/predicted funding rates from all exchanges.
    Writes ONLY to current_rates (for live dashboard).
    Does NOT write to funding_rates — settlements handle that.
    Uses batch endpoints only — exchanges without batch are updated by settlement job.
    All exchanges run concurrently."""
    logger.info("Starting snapshot collection...")
    now_ms = int(time.time() * 1000)

    exchanges = get_all_exchanges()

    async def _collect_exchange(name: str, ex):
        try:
            batch_rates = await ex.fetch_current_rates_batch()
            if batch_rates:
                current_rows = [(name, sym, rate, now_ms) for sym, rate in batch_rates]
                await upsert_current_rates(current_rows)
                logger.info(f"[{name}] snapshot batch: {len(batch_rates)} rates")
            # No per-symbol fallback — settlement job updates current_rates for exchanges without batch
        except Exception as e:
            logger.error(f"[{name}] snapshot failed: {e}")

    all_tasks = [
        asyncio.create_task(_collect_exchange(name, ex))
        for name, ex in exchanges.items()
    ]
    await asyncio.gather(*all_tasks, return_exceptions=True)

    logger.info("Snapshot collection complete.")


async def collect_settlements():
    """Hourly: fetch actual settlement rates from all exchanges.
    Uses fetch_funding_history() which returns real settlement timestamps.
    INSERT OR IGNORE deduplicates overlapping lookback windows.
    Also updates current_rates with the latest settlement per symbol."""
    logger.info("Starting settlement collection...")
    now_ms = int(time.time() * 1000)
    lookback_ms = SETTLEMENT_LOOKBACK_HOURS * 3_600_000
    start_ms = now_ms - lookback_ms

    exchanges = get_all_exchanges()

    async def _collect_exchange(name: str, ex):
        try:
            symbols = await get_exchange_symbols(name)
            if not symbols:
                return

            sem = asyncio.Semaphore(SETTLEMENT_CONCURRENCY)
            funding_rows = []
            current_rows = []

            async def _fetch_one(sym, raw_sym):
                async with sem:
                    try:
                        rates = await ex.fetch_funding_history(raw_sym, start_ms, now_ms)
                        for r in rates:
                            funding_rows.append((name, sym, r["timestamp"], r["rate"]))
                        if rates:
                            latest = max(rates, key=lambda x: x["timestamp"])
                            current_rows.append((name, sym, latest["rate"], latest["timestamp"]))
                    except Exception:
                        pass
                    await asyncio.sleep(SETTLEMENT_DELAY)

            tasks = [asyncio.create_task(_fetch_one(s, r)) for s, r in symbols]
            await asyncio.gather(*tasks, return_exceptions=True)

            if funding_rows:
                await upsert_funding_rates(funding_rows)
            if current_rows:
                await upsert_current_rates(current_rows)
            logger.info(f"[{name}] settlements: {len(funding_rows)} rates")

        except Exception as e:
            logger.error(f"[{name}] settlement collection failed: {e}")

    all_tasks = [
        asyncio.create_task(_collect_exchange(name, ex))
        for name, ex in exchanges.items()
    ]
    await asyncio.gather(*all_tasks, return_exceptions=True)

    await rebuild_current_rates()
    logger.info("Settlement collection complete.")


async def backfill_exchange(exchange_name: str, days_back: int = 60):
    """Backfill historical data for a single exchange."""
    ex = get_exchange(exchange_name)
    if ex is None:
        logger.error(f"Unknown exchange: {exchange_name}")
        return

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - (days_back * 24 * 3600 * 1000)

    # Refresh symbols first
    await refresh_symbols(exchange_name)
    symbols = await get_exchange_symbols(exchange_name)
    if not symbols:
        logger.warning(f"[{exchange_name}] no symbols to backfill")
        return

    logger.info(f"[{exchange_name}] backfilling {len(symbols)} symbols for {days_back} days...")
    total_rates = 0
    current_rows = []

    for i, (sym, raw_sym) in enumerate(symbols):
        try:
            rates = await ex.fetch_funding_history(raw_sym, start_ms, now_ms)
            if rates:
                rows = [(exchange_name, sym, r["timestamp"], r["rate"]) for r in rates]
                await upsert_funding_rates(rows)
                total_rates += len(rates)
                latest = max(rates, key=lambda x: x["timestamp"])
                current_rows.append((exchange_name, sym, latest["rate"], latest["timestamp"]))

            if (i + 1) % 50 == 0:
                logger.info(f"[{exchange_name}] progress: {i+1}/{len(symbols)} symbols, {total_rates} rates")

        except Exception as e:
            logger.debug(f"[{exchange_name}] {sym} backfill error: {e}")

        await asyncio.sleep(REQUEST_DELAY)

    if current_rows:
        await upsert_current_rates(current_rows)

    logger.info(f"[{exchange_name}] backfill complete: {total_rates} total rates from {len(symbols)} symbols")


async def backfill_all(days_back: int = 60, exchanges: list[str] | None = None):
    """Backfill all exchanges. Runs BACKFILL_CONCURRENCY exchanges in parallel."""
    all_exchanges = get_all_exchanges()
    names = exchanges if exchanges else list(all_exchanges.keys())

    logger.info(f"Starting backfill for {len(names)} exchanges, {days_back} days back...")

    sem = asyncio.Semaphore(BACKFILL_CONCURRENCY)

    async def _backfill_with_sem(name: str):
        async with sem:
            await backfill_exchange(name, days_back)

    tasks = [asyncio.create_task(_backfill_with_sem(name)) for name in names]
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Backfill complete for all exchanges.")
