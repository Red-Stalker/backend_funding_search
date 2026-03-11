import asyncio
import logging
import time

from config import REQUEST_DELAY, COLLECTOR_LOOKBACK_HOURS, BACKFILL_CONCURRENCY, SNAPSHOT_CONCURRENCY
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


async def collect_latest():
    """Hourly job: fetch recent funding rates from all exchanges."""
    logger.info("Starting hourly collection...")
    now_ms = int(time.time() * 1000)
    lookback_ms = COLLECTOR_LOOKBACK_HOURS * 3600 * 1000
    start_ms = now_ms - lookback_ms

    exchanges = get_all_exchanges()

    for name, ex in exchanges.items():
        try:
            symbols = await get_exchange_symbols(name)
            if not symbols:
                # Try refreshing symbols first
                await refresh_symbols(name)
                symbols = await get_exchange_symbols(name)
            if not symbols:
                continue

            logger.info(f"[{name}] collecting {len(symbols)} symbols...")
            funding_rows = []
            current_rows = []

            for sym, raw_sym in symbols:
                try:
                    rates = await ex.fetch_funding_history(raw_sym, start_ms, now_ms)
                    for r in rates:
                        funding_rows.append((name, sym, r["timestamp"], r["rate"]))
                    # Use the latest rate as current
                    if rates:
                        latest = max(rates, key=lambda x: x["timestamp"])
                        current_rows.append((name, sym, latest["rate"], latest["timestamp"]))
                except Exception as e:
                    logger.debug(f"[{name}] {sym} failed: {e}")
                await asyncio.sleep(REQUEST_DELAY)

            if funding_rows:
                await upsert_funding_rates(funding_rows)
            if current_rows:
                await upsert_current_rates(current_rows)

            logger.info(f"[{name}] collected {len(funding_rows)} rate points, {len(current_rows)} current rates")

        except Exception as e:
            logger.error(f"[{name}] collection failed: {e}")

    await rebuild_current_rates()
    logger.info("Hourly collection complete.")


async def collect_snapshots():
    """Snapshot job: fetch current/predicted funding rates from all exchanges.
    Runs every ~5 minutes to match loris.tools frequency.
    Uses batch endpoints where available, falls back to per-symbol.
    All exchanges run concurrently."""
    logger.info("Starting snapshot collection...")
    now_ms = int(time.time() * 1000)

    exchanges = get_all_exchanges()

    async def _collect_exchange(name: str, ex):
        try:
            # Try batch endpoint first
            batch_rates = await ex.fetch_current_rates_batch()
            if batch_rates:
                funding_rows = [(name, sym, now_ms, rate) for sym, rate in batch_rates]
                current_rows = [(name, sym, rate, now_ms) for sym, rate in batch_rates]
                await upsert_funding_rates(funding_rows)
                await upsert_current_rates(current_rows)
                logger.info(f"[{name}] snapshot batch: {len(batch_rates)} rates")
                return

            # Fallback: per-symbol with limited concurrency
            symbols = await get_exchange_symbols(name)
            if not symbols:
                return

            sem = asyncio.Semaphore(3)
            funding_rows = []
            current_rows = []

            async def _fetch_one(sym, raw_sym):
                async with sem:
                    try:
                        rates = await ex.fetch_funding_history(raw_sym, now_ms - 3600_000, now_ms)
                        if rates:
                            latest = max(rates, key=lambda x: x["timestamp"])
                            funding_rows.append((name, sym, now_ms, latest["rate"]))
                            current_rows.append((name, sym, latest["rate"], now_ms))
                    except Exception:
                        pass
                    await asyncio.sleep(REQUEST_DELAY)

            tasks = [asyncio.create_task(_fetch_one(s, r)) for s, r in symbols]
            await asyncio.gather(*tasks, return_exceptions=True)

            if funding_rows:
                await upsert_funding_rates(funding_rows)
            if current_rows:
                await upsert_current_rates(current_rows)
            if funding_rows:
                logger.info(f"[{name}] snapshot per-symbol: {len(funding_rows)} rates")

        except Exception as e:
            logger.error(f"[{name}] snapshot failed: {e}")

    all_tasks = [
        asyncio.create_task(_collect_exchange(name, ex))
        for name, ex in exchanges.items()
    ]
    await asyncio.gather(*all_tasks, return_exceptions=True)

    logger.info("Snapshot collection complete.")


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
