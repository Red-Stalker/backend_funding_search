"""
Bulk download historical funding rate data from loris.tools.

Usage:
    python loris_download.py --days 60                  # All symbols, 60 days
    python loris_download.py --days 30 --symbols BTC,ETH,SOL
    python loris_download.py --status                   # Show DB coverage
    python loris_download.py --exchanges                # Show loris exchanges
    python loris_download.py --days 60 --concurrency 20 # Faster download

First-time setup (~30-60 minutes for 60 days):
    python loris_download.py --days 60
"""
import argparse
import asyncio
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from database import (
    init_db,
    upsert_funding_rates,
    upsert_current_rates,
    get_data_coverage,
    get_latest_ts_per_symbol,
    get_db_stats,
)
from loris_client import LorisClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Download in 1-day chunks — loris downsamples responses for ranges > 1 day.
CHUNK_DAYS = 1

# Batch DB writes — accumulate rows and flush periodically
DB_FLUSH_THRESHOLD = 50_000


def _parse_series_to_rows(
    symbol: str, series: dict[str, list[dict]]
) -> tuple[list[tuple], list[tuple]]:
    """Convert loris series to DB rows.
    Returns (funding_rows, current_rows).
    """
    funding_rows = []
    current_rows = []

    for exchange, points in series.items():
        latest_ts = 0
        latest_rate = 0.0

        for pt in points:
            try:
                dt = datetime.fromisoformat(pt["t"].replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                rate = float(pt["y"])
                funding_rows.append((exchange, symbol, ts_ms, rate))
                if ts_ms > latest_ts:
                    latest_ts = ts_ms
                    latest_rate = rate
            except (KeyError, ValueError):
                continue

        if latest_ts:
            current_rows.append((exchange, symbol, latest_rate, latest_ts))

    return funding_rows, current_rows


async def show_status():
    """Show database coverage status."""
    await init_db()
    stats = await get_db_stats()

    if stats["total_records"] == 0:
        print("\nDatabase is empty. Run: python loris_download.py --days 60")
        return

    print(f"\n{'='*80}")
    print(f"DATABASE STATUS")
    print(f"{'='*80}")
    print(f"Total records:  {stats['total_records']:>12,}")
    print(f"Exchanges:      {stats['exchanges']:>12}")
    print(f"Symbols:        {stats['symbols']:>12}")

    if stats["oldest_ts"] and stats["newest_ts"]:
        oldest = datetime.fromtimestamp(stats["oldest_ts"] / 1000, tz=timezone.utc)
        newest = datetime.fromtimestamp(stats["newest_ts"] / 1000, tz=timezone.utc)
        span = newest - oldest
        print(f"Date range:     {oldest.strftime('%Y-%m-%d %H:%M')} -- {newest.strftime('%Y-%m-%d %H:%M')} ({span.days} days)")

    print(f"\n{'Exchange':<20} {'Symbols':>8} {'Records':>12} {'Oldest':>20} {'Newest':>20}")
    print("-" * 84)

    coverage = await get_data_coverage()
    total_points = 0
    for row in sorted(coverage, key=lambda x: -x["total_points"]):
        oldest = datetime.fromtimestamp(row["oldest_ts"] / 1000, tz=timezone.utc)
        newest = datetime.fromtimestamp(row["newest_ts"] / 1000, tz=timezone.utc)
        total_points += row["total_points"]
        print(
            f"{row['exchange']:<20} {row['symbols_count']:>8} "
            f"{row['total_points']:>12,} "
            f"{oldest.strftime('%Y-%m-%d %H:%M'):>20} "
            f"{newest.strftime('%Y-%m-%d %H:%M'):>20}"
        )

    print("-" * 84)
    print(f"{'TOTAL':<20} {'':>8} {total_points:>12,}")
    print()


async def show_exchanges():
    """Show exchanges available on loris.tools."""
    client = LorisClient()
    try:
        exchanges = await client.list_exchanges()
        if not exchanges:
            print("Failed to fetch exchanges from loris.tools")
            return

        print(f"\nLoris.tools tracks {len(exchanges)} exchanges:\n")
        print(f"{'Exchange':<20} {'Symbols':>8}")
        print("-" * 30)
        for ex in sorted(exchanges.keys()):
            print(f"{ex:<20} {exchanges[ex]:>8}")
        print(f"\nTotal symbols across all exchanges: {sum(exchanges.values()):,}")
    finally:
        await client.close()


async def main():
    parser = argparse.ArgumentParser(
        description="Download funding rate history from loris.tools"
    )
    parser.add_argument(
        "--days", type=int, default=60,
        help="Days of history to download (default: 60)",
    )
    parser.add_argument(
        "--symbols", type=str, default=None,
        help="Comma-separated symbols (default: all from loris)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=15,
        help="Concurrent download requests (default: 15)",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show database coverage status",
    )
    parser.add_argument(
        "--exchanges", action="store_true",
        help="Show available exchanges from loris.tools",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Ignore freshness check — re-download even if data exists",
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Start date ISO (e.g. 2026-03-03), overrides --days",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="End date ISO (e.g. 2026-03-07), default: now",
    )
    args = parser.parse_args()

    if args.status:
        await show_status()
        return

    if args.exchanges:
        await show_exchanges()
        return

    # --- Download mode ---
    await init_db()

    client = LorisClient()
    try:
        # Get symbol list
        if args.symbols:
            symbols = [s.strip().upper() for s in args.symbols.split(",")]
        else:
            logger.info("Fetching symbol list from loris.tools...")
            symbols = await client.fetch_symbols()
            if not symbols:
                logger.error("Failed to fetch symbols from loris.tools -- check proxies")
                return
            logger.info(f"Got {len(symbols)} symbols from loris")

        # Time range
        if args.start:
            start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        else:
            start = datetime.now(timezone.utc) - timedelta(days=args.days)

        if args.end:
            end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        else:
            end = datetime.now(timezone.utc)

        # Smart download: check what we already have (skip if --force)
        existing = {} if args.force else await get_latest_ts_per_symbol()

        # Build flat task list: (symbol, chunk_start, chunk_end)
        tasks_list = []
        skipped = 0

        for symbol in symbols:
            sym_start = start
            if symbol in existing:
                latest_dt = datetime.fromtimestamp(
                    existing[symbol] / 1000, tz=timezone.utc
                )
                if (end - latest_dt).total_seconds() < 600:
                    skipped += 1
                    continue
                # Download only the gap
                sym_start = latest_dt

            chunk_start = sym_start
            while chunk_start < end:
                chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end)
                tasks_list.append((symbol, chunk_start, chunk_end))
                chunk_start = chunk_end

        total_tasks = len(tasks_list)
        logger.info(
            f"Download plan: {len(symbols)} symbols, {args.days} days, "
            f"{total_tasks:,} chunks, concurrency={args.concurrency}, "
            f"{skipped} skipped (fresh)"
        )

        if not tasks_list:
            logger.info("Nothing to download -- all data is up to date")
            await show_status()
            return

        # Shared state
        sem = asyncio.Semaphore(args.concurrency)
        completed = 0
        total_rows = 0
        failed = 0
        start_time = time.time()

        # Batched DB writes
        pending_funding = []
        pending_current = []
        db_lock = asyncio.Lock()

        async def _flush_db():
            nonlocal pending_funding, pending_current
            async with db_lock:
                if pending_funding:
                    rows = pending_funding[:]
                    pending_funding = []
                    await upsert_funding_rates(rows)
                if pending_current:
                    rows = pending_current[:]
                    pending_current = []
                    await upsert_current_rates(rows)

        async def _download_chunk(symbol: str, c_start: datetime, c_end: datetime):
            nonlocal completed, total_rows, failed, pending_funding, pending_current
            async with sem:
                try:
                    series = await client.fetch_historical(symbol, c_start, c_end)
                    if series:
                        funding_rows, current_rows = _parse_series_to_rows(symbol, series)
                        if funding_rows:
                            flush_fr = None
                            flush_cr = None
                            async with db_lock:
                                pending_funding.extend(funding_rows)
                                pending_current.extend(current_rows)
                                total_rows += len(funding_rows)
                                if len(pending_funding) >= DB_FLUSH_THRESHOLD:
                                    flush_fr = pending_funding[:]
                                    flush_cr = pending_current[:]
                                    pending_funding = []
                                    pending_current = []
                            if flush_fr:
                                await upsert_funding_rates(flush_fr)
                            if flush_cr:
                                await upsert_current_rates(flush_cr)
                except Exception:
                    failed += 1

                completed += 1
                if completed % 100 == 0 or completed == total_tasks:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total_tasks - completed) / rate if rate > 0 else 0
                    logger.info(
                        f"Progress: {completed:,}/{total_tasks:,} chunks "
                        f"({total_rows:,} rows, {failed} failed, "
                        f"ETA: {eta:.0f}s / {eta/60:.1f}min)"
                    )

        # Launch all tasks
        coros = [
            asyncio.create_task(_download_chunk(sym, cs, ce))
            for sym, cs, ce in tasks_list
        ]
        await asyncio.gather(*coros, return_exceptions=True)

        # Final flush
        await _flush_db()

        elapsed = time.time() - start_time
        logger.info(
            f"\nDownload complete in {elapsed:.0f}s ({elapsed/60:.1f} min)!\n"
            f"  Chunks processed: {completed:,}\n"
            f"  Rows inserted:    {total_rows:,}\n"
            f"  Skipped (fresh):  {skipped}\n"
            f"  Failed:           {failed}"
        )

        print()
        await show_status()

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
