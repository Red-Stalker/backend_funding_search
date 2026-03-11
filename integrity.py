"""
Data integrity check and gap filling from loris.tools.

On server startup:
1. Checks the latest data timestamp in DB
2. If gap > INTEGRITY_GAP_MINUTES, fills missing data from loris.tools
3. If DB is empty, warns the user to run loris_download.py first

Deep integrity check (periodic):
1. Per-exchange staleness detection
2. Time-bucket gap detection (2h buckets)
3. Targeted gap filling from loris.tools

On Windows, runs as a subprocess to avoid uvicorn event loop conflicts.
"""
import asyncio
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from config import INTEGRITY_GAP_MINUTES, DEEP_INTEGRITY_WINDOW_DAYS
from database import (
    get_global_latest_ts,
    get_all_symbols,
    get_per_exchange_latest_ts,
    get_exchange_bucket_counts,
    get_sparse_exchange_symbols,
    get_exchange_symbols,
    get_symbol_point_counts,
    get_coverage_gaps,
    get_thin_buckets,
    upsert_funding_rates,
    upsert_current_rates,
    rebuild_current_rates,
)

logger = logging.getLogger(__name__)

BACKEND_DIR = Path(__file__).resolve().parent


def _parse_loris_points(exchange: str, symbol: str,
                        points: list[dict]) -> tuple[list[tuple], tuple | None]:
    """Convert loris [{t, y}] to DB rows."""
    funding_rows = []
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

    current_row = (exchange, symbol, latest_rate, latest_ts) if latest_ts else None
    return funding_rows, current_row


async def check_and_fill_gaps():
    """Check data integrity and fill gaps.

    Detects whether we're inside uvicorn (ProactorEventLoop on Windows)
    and runs as subprocess if needed, otherwise runs inline.
    """
    latest_ts = await get_global_latest_ts()
    now_ms = int(time.time() * 1000)

    if latest_ts is None:
        logger.warning(
            "Database is EMPTY. Run 'python loris_download.py --days 60' "
            "to download initial historical data from loris.tools"
        )
        await _try_fill_via_subprocess_or_inline(empty_db=True)
        return

    gap_ms = now_ms - latest_ts
    gap_minutes = gap_ms / (60 * 1000)

    if gap_minutes <= INTEGRITY_GAP_MINUTES:
        logger.info(
            f"Integrity OK — latest data is {gap_minutes:.1f} min old"
        )
        return

    gap_hours = gap_minutes / 60
    logger.info(
        f"Data gap detected: {gap_hours:.1f}h ({gap_minutes:.0f} min). "
        f"Filling from loris.tools..."
    )

    await _try_fill_via_subprocess_or_inline(empty_db=False)


async def _try_fill_via_subprocess_or_inline(empty_db: bool):
    """Try subprocess first (reliable on Windows), fallback to inline."""
    # Always use subprocess approach — it works reliably on both
    # Windows (where uvicorn event loop conflicts with aiohttp+proxy)
    # and Linux.
    try:
        await _fill_gaps_subprocess(empty_db)
    except Exception as e:
        logger.warning(f"Subprocess gap fill failed ({e}), trying inline...")
        try:
            if empty_db:
                await _fill_current_snapshot_inline()
            else:
                await _fill_gaps_inline()
        except Exception as e2:
            logger.error(f"Inline gap fill also failed: {e2}")


async def _fill_gaps_subprocess(empty_db: bool):
    """Run gap filling as a subprocess.

    Uses subprocess.run in a thread (via asyncio.to_thread) instead of
    asyncio.create_subprocess_exec, because the latter requires
    ProactorEventLoop on Windows while we use SelectorEventLoop for aiohttp.
    """
    import subprocess as sp

    script = str(BACKEND_DIR / "_integrity_worker.py")

    # Write a small worker script that runs the actual fill logic
    worker_code = (
        '# -*- coding: utf-8 -*-\n'
        'import asyncio, sys, logging, time\n'
        'from datetime import datetime, timezone\n'
        'if sys.platform == "win32":\n'
        '    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())\n'
        'logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")\n'
        'logger = logging.getLogger("integrity.worker")\n'
        'from database import init_db, get_global_latest_ts, get_all_symbols\n'
        'from database import upsert_funding_rates, upsert_current_rates, rebuild_current_rates\n'
        'from loris_client import LorisClient\n'
        'async def fill_gaps():\n'
        '    await init_db()\n'
        '    latest_ts = await get_global_latest_ts()\n'
        '    now_ms = int(time.time() * 1000)\n'
        '    client = LorisClient()\n'
        '    try:\n'
        '        if latest_ts is None:\n'
        '            logger.info("DB empty, fetching current snapshot from loris")\n'
        '            rates = await client.fetch_current_rates()\n'
        '            if not rates:\n'
        '                logger.warning("Could not fetch from loris")\n'
        '                return\n'
        '            fr = []\n'
        '            cr = []\n'
        '            for ex, syms in rates.items():\n'
        '                for sym, rate in syms.items():\n'
        '                    fr.append((ex, sym, now_ms, rate))\n'
        '                    cr.append((ex, sym, rate, now_ms))\n'
        '            if fr:\n'
        '                await upsert_funding_rates(fr)\n'
        '                await upsert_current_rates(cr)\n'
        '                logger.info(f"Loaded {len(fr)} current rates")\n'
        '            return\n'
        '        symbols = await get_all_symbols()\n'
        '        if not symbols:\n'
        '            logger.warning("No symbols in DB")\n'
        '            return\n'
        '        start = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)\n'
        '        end = datetime.now(timezone.utc)\n'
        '        gap_min = (now_ms - latest_ts) / 60000\n'
        '        logger.info(f"Filling {gap_min:.0f} min gap for {len(symbols)} symbols...")\n'
        '        total = 0\n'
        '        sem = asyncio.Semaphore(3)\n'
        '        async def _fill(symbol):\n'
        '            nonlocal total\n'
        '            async with sem:\n'
        '                series = await client.fetch_historical(symbol, start, end)\n'
        '                if not series:\n'
        '                    return\n'
        '                rows = []\n'
        '                for exchange, points in series.items():\n'
        '                    for pt in points:\n'
        '                        try:\n'
        '                            dt = datetime.fromisoformat(pt["t"].replace("Z", "+00:00"))\n'
        '                            ts_ms = int(dt.timestamp() * 1000)\n'
        '                            rate = float(pt["y"])\n'
        '                            rows.append((exchange, symbol, ts_ms, rate))\n'
        '                        except (KeyError, ValueError):\n'
        '                            continue\n'
        '                if rows:\n'
        '                    await upsert_funding_rates(rows)\n'
        '                    total += len(rows)\n'
        '                await asyncio.sleep(0.3)\n'
        '        tasks = [asyncio.create_task(_fill(s)) for s in symbols]\n'
        '        await asyncio.gather(*tasks, return_exceptions=True)\n'
        '        await rebuild_current_rates()\n'
        '        logger.info(f"Gap fill done: {total:,} records")\n'
        '    finally:\n'
        '        await client.close()\n'
        'asyncio.run(fill_gaps())\n'
    )

    with open(script, "w") as f:
        f.write(worker_code)

    try:
        logger.info("Running integrity worker subprocess...")

        def _run_worker():
            return sp.run(
                [sys.executable, script],
                cwd=str(BACKEND_DIR),
                capture_output=True,
                text=True,
                timeout=300,
            )

        result = await asyncio.to_thread(_run_worker)
        output = (result.stdout or "") + (result.stderr or "")
        output = output.strip()
        if output:
            for line in output.split("\n"):
                logger.info(f"[worker] {line}")
        if result.returncode != 0:
            logger.warning(f"Integrity worker exited with code {result.returncode}")
        else:
            logger.info("Integrity worker completed successfully")
    finally:
        # Clean up temp worker script
        try:
            Path(script).unlink(missing_ok=True)
        except Exception:
            pass


async def _fill_gaps_inline():
    """Inline gap filling — used as fallback if subprocess fails."""
    from loris_client import LorisClient

    latest_ts = await get_global_latest_ts()
    if latest_ts is None:
        return

    symbols = await get_all_symbols()
    if not symbols:
        return

    client = LorisClient()
    try:
        start = datetime.fromtimestamp(latest_ts / 1000, tz=timezone.utc)
        end = datetime.now(timezone.utc)

        total_inserted = 0
        sem = asyncio.Semaphore(3)

        async def _fill_symbol(symbol: str):
            nonlocal total_inserted
            async with sem:
                series = await client.fetch_historical(symbol, start, end)
                if not series:
                    return

                all_funding = []
                all_current = []
                for exchange, points in series.items():
                    funding_rows, current_row = _parse_loris_points(
                        exchange, symbol, points
                    )
                    all_funding.extend(funding_rows)
                    if current_row:
                        all_current.append(current_row)

                if all_funding:
                    await upsert_funding_rates(all_funding)
                    total_inserted += len(all_funding)
                if all_current:
                    await upsert_current_rates(all_current)

                await asyncio.sleep(0.3)

        tasks = [asyncio.create_task(_fill_symbol(sym)) for sym in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        await rebuild_current_rates()

        logger.info(f"Inline gap fill: {total_inserted:,} records")
    finally:
        await client.close()


async def _fill_current_snapshot_inline():
    """Inline: grab current rates from loris."""
    from loris_client import LorisClient

    client = LorisClient()
    try:
        rates = await client.fetch_current_rates()
        if not rates:
            return

        now_ms = int(time.time() * 1000)
        funding_rows = []
        current_rows = []

        for exchange, symbols in rates.items():
            for symbol, rate in symbols.items():
                funding_rows.append((exchange, symbol, now_ms, rate))
                current_rows.append((exchange, symbol, rate, now_ms))

        if funding_rows:
            await upsert_funding_rates(funding_rows)
            await upsert_current_rates(current_rows)
            logger.info(
                f"Loaded current snapshot: {len(funding_rows)} rates "
                f"from {len(rates)} exchanges"
            )
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Deep integrity check — per-exchange gap detection + loris fill
# ---------------------------------------------------------------------------

BUCKET_MS = 7_200_000             # 2-hour buckets
STALENESS_THRESHOLD_HOURS = 3
LORIS_CONCURRENCY = 5

_loris_exchanges: set[str] | None = None


async def _get_loris_exchanges() -> set[str]:
    """Cached set of exchange names available on loris.tools."""
    global _loris_exchanges
    if _loris_exchanges is not None:
        return _loris_exchanges
    from loris_client import LorisClient
    client = LorisClient()
    try:
        mapping = await client.list_exchanges()
        _loris_exchanges = set(mapping.keys())
        logger.info(f"Loris exchanges: {len(_loris_exchanges)}")
    except Exception:
        _loris_exchanges = set()
    finally:
        await client.close()
    return _loris_exchanges

_last_report: dict | None = None


def get_last_report() -> dict | None:
    """Return the last deep integrity check report."""
    return _last_report


def _find_gaps_in_buckets(
    bucket_counts: dict[int, int],
    first_bucket: int,
    last_bucket: int,
) -> list[tuple[int, int]]:
    """Find contiguous ranges of missing buckets.
    Returns [(start_ms, end_ms), ...] for each gap.
    """
    gaps = []
    gap_start = None

    for b in range(first_bucket, last_bucket + 1):
        if b not in bucket_counts:
            if gap_start is None:
                gap_start = b
        else:
            if gap_start is not None:
                gaps.append((gap_start * BUCKET_MS, b * BUCKET_MS))
                gap_start = None

    if gap_start is not None:
        gaps.append((gap_start * BUCKET_MS, (last_bucket + 1) * BUCKET_MS))

    return gaps


def _ts_in_any_gap(ts_ms: int, gaps: list[tuple[int, int]]) -> bool:
    """Check if a timestamp falls inside any gap range."""
    for start, end in gaps:
        if start <= ts_ms <= end:
            return True
    return False


async def _fill_gaps_from_loris(
    gaps_by_exchange: dict[str, list[tuple[int, int]]],
    all_symbols: list[str],
) -> dict[str, int]:
    """Fill detected gaps from loris.tools.
    Skips exchanges not available on loris.
    Returns {exchange: rows_inserted}.
    """
    from loris_client import LorisClient

    # Filter to exchanges that exist on loris
    loris_exs = await _get_loris_exchanges()
    gaps_by_exchange = {
        ex: gaps for ex, gaps in gaps_by_exchange.items()
        if ex in loris_exs
    }
    if not gaps_by_exchange:
        return {}

    # Compute global time window across all gaps
    global_start = min(
        g[0] for gaps in gaps_by_exchange.values() for g in gaps
    )
    global_end = max(
        g[1] for gaps in gaps_by_exchange.values() for g in gaps
    )

    start_dt = datetime.fromtimestamp(global_start / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(global_end / 1000, tz=timezone.utc)

    exchanges_with_gaps = set(gaps_by_exchange.keys())
    inserted: dict[str, int] = {ex: 0 for ex in exchanges_with_gaps}

    # Narrow symbols to those actually traded on gapped exchanges
    symbols_to_fetch: set[str] = set()
    for ex in exchanges_with_gaps:
        ex_syms = await get_exchange_symbols(ex)
        symbols_to_fetch.update(sym for sym, _raw in ex_syms)
    # Intersect with known symbols (safety)
    symbols_to_fetch &= set(all_symbols)

    if not symbols_to_fetch:
        logger.info("No symbols to fetch for gapped exchanges")
        return inserted

    logger.info(
        f"Fetching {len(symbols_to_fetch)} symbols from loris "
        f"for {len(exchanges_with_gaps)} gapped exchanges"
    )

    client = LorisClient()
    try:
        sem = asyncio.Semaphore(LORIS_CONCURRENCY)

        async def _fill_symbol(symbol: str):
            async with sem:
                series = await client.fetch_historical(symbol, start_dt, end_dt)
                if not series:
                    return

                rows = []
                for exchange, points in series.items():
                    if exchange not in exchanges_with_gaps:
                        continue
                    ex_gaps = gaps_by_exchange[exchange]
                    for pt in points:
                        try:
                            dt = datetime.fromisoformat(
                                pt["t"].replace("Z", "+00:00")
                            )
                            ts_ms = int(dt.timestamp() * 1000)
                            rate = float(pt["y"])
                            if _ts_in_any_gap(ts_ms, ex_gaps):
                                rows.append((exchange, symbol, ts_ms, rate))
                        except (KeyError, ValueError):
                            continue

                if rows:
                    await upsert_funding_rates(rows)
                    for ex, _sym, _ts, _r in rows:
                        inserted[ex] = inserted.get(ex, 0) + 1

                await asyncio.sleep(0.3)

        tasks = [asyncio.create_task(_fill_symbol(s)) for s in symbols_to_fetch]
        await asyncio.gather(*tasks, return_exceptions=True)
        await rebuild_current_rates()
    finally:
        await client.close()

    return inserted


async def _fill_thin_buckets_from_loris(
    thin_buckets: list[tuple[int, int]],
    all_symbols: list[str],
) -> int:
    """Fill thin buckets by fetching from loris with narrow time windows.
    Narrow windows ensure loris returns 5-min resolution (not hourly).
    Returns total rows inserted.
    """
    from loris_client import LorisClient

    total = 0
    client = LorisClient()
    try:
        sem = asyncio.Semaphore(LORIS_CONCURRENCY)

        for bucket_start, bucket_end in thin_buckets:
            start_dt = datetime.fromtimestamp(
                bucket_start / 1000, tz=timezone.utc
            )
            end_dt = datetime.fromtimestamp(
                bucket_end / 1000, tz=timezone.utc
            )
            logger.info(
                f"Filling thin bucket "
                f"{start_dt.strftime('%m-%d %H:%M')} -> "
                f"{end_dt.strftime('%m-%d %H:%M')}"
            )

            async def _fill(symbol: str):
                nonlocal total
                async with sem:
                    series = await client.fetch_historical(
                        symbol, start_dt, end_dt
                    )
                    if not series:
                        return
                    rows = []
                    for exchange, points in series.items():
                        for pt in points:
                            try:
                                dt = datetime.fromisoformat(
                                    pt["t"].replace("Z", "+00:00")
                                )
                                ts_ms = int(dt.timestamp() * 1000)
                                rate = float(pt["y"])
                                rows.append(
                                    (exchange, symbol, ts_ms, rate)
                                )
                            except (KeyError, ValueError):
                                continue
                    if rows:
                        await upsert_funding_rates(rows)
                        total += len(rows)
                    await asyncio.sleep(0.3)

            tasks = [
                asyncio.create_task(_fill(s)) for s in all_symbols
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await client.close()

    return total


COVERAGE_THRESHOLD = 0.5   # exchange with < 50% of best -> "underfilled"
COVERAGE_MIN_BEST = 50     # symbol must have >= 50 points on best exchange


def _find_underfilled_symbols(
    symbol_counts: dict[str, dict[str, int]],
) -> dict[str, list[str]]:
    """Find (exchange, symbol) pairs with significantly fewer points than the best.
    Returns {exchange: [symbol, ...]}.
    """
    underfilled: dict[str, list[str]] = {}

    for symbol, ex_counts in symbol_counts.items():
        if len(ex_counts) < 2:
            continue
        best_count = max(ex_counts.values())
        if best_count < COVERAGE_MIN_BEST:
            continue
        threshold = best_count * COVERAGE_THRESHOLD

        for exchange, count in ex_counts.items():
            if count < threshold:
                if exchange not in underfilled:
                    underfilled[exchange] = []
                underfilled[exchange].append(symbol)

    return underfilled


async def _fill_underfilled_from_loris(
    underfilled: dict[str, list[str]],
    start_ms: int,
    end_ms: int,
) -> dict[str, int]:
    """Re-fetch from loris for (exchange, symbol) pairs that are underfilled.
    Skips exchanges not available on loris.
    INSERT OR IGNORE ensures we only add missing rows.
    Returns {exchange: rows_inserted}.
    """
    from loris_client import LorisClient

    # Filter to exchanges on loris
    loris_exs = await _get_loris_exchanges()
    underfilled = {
        ex: syms for ex, syms in underfilled.items()
        if ex in loris_exs
    }
    if not underfilled:
        return {}

    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)

    # Collect unique symbols to fetch
    symbols_to_fetch = set()
    for syms in underfilled.values():
        symbols_to_fetch.update(syms)

    exchanges_to_fill = set(underfilled.keys())
    inserted: dict[str, int] = {ex: 0 for ex in exchanges_to_fill}

    logger.info(
        f"Fetching {len(symbols_to_fetch)} unique symbols from loris "
        f"for {len(exchanges_to_fill)} exchanges"
    )

    client = LorisClient()
    try:
        sem = asyncio.Semaphore(LORIS_CONCURRENCY)

        async def _fill_symbol(symbol: str):
            async with sem:
                series = await client.fetch_historical(
                    symbol, start_dt, end_dt
                )
                if not series:
                    return

                rows = []
                for exchange, points in series.items():
                    if exchange not in exchanges_to_fill:
                        continue
                    if symbol not in underfilled.get(exchange, []):
                        continue
                    for pt in points:
                        try:
                            dt = datetime.fromisoformat(
                                pt["t"].replace("Z", "+00:00")
                            )
                            ts_ms = int(dt.timestamp() * 1000)
                            rate = float(pt["y"])
                            rows.append((exchange, symbol, ts_ms, rate))
                        except (KeyError, ValueError):
                            continue

                if rows:
                    await upsert_funding_rates(rows)
                    for ex, _sym, _ts, _r in rows:
                        inserted[ex] = inserted.get(ex, 0) + 1

                await asyncio.sleep(0.3)

        tasks = [
            asyncio.create_task(_fill_symbol(s))
            for s in symbols_to_fetch
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await client.close()

    return inserted


async def _backfill_sparse_symbols(
    exchange_latest: dict[str, int],
    start_ms: int,
    now_ms: int,
    min_points: int = 10,
) -> dict[str, int]:
    """Find symbols with incomplete history and backfill from exchange API.

    For each exchange, finds symbols with fewer than min_points in the window,
    then calls fetch_funding_history() directly on the exchange to fill gaps.

    Returns {exchange: symbols_backfilled}.
    """
    from exchanges.registry import get_exchange

    backfilled: dict[str, int] = {}

    for exchange_name in exchange_latest:
        ex = get_exchange(exchange_name)
        if ex is None:
            continue

        sparse = await get_sparse_exchange_symbols(
            exchange_name, start_ms, min_points
        )
        if not sparse:
            continue

        # Get raw symbol mapping
        sym_map = dict(await get_exchange_symbols(exchange_name))
        to_fill = [(sym, sym_map[sym]) for sym in sparse if sym in sym_map]

        if not to_fill:
            continue

        logger.info(
            f"[{exchange_name}] backfilling {len(to_fill)} sparse symbols "
            f"(< {min_points} pts in window)"
        )

        sem = asyncio.Semaphore(3)
        filled_count = 0

        async def _fill_one(sym: str, raw_sym: str):
            nonlocal filled_count
            async with sem:
                try:
                    rates = await ex.fetch_funding_history(
                        raw_sym, start_ms, now_ms
                    )
                    if rates:
                        rows = [
                            (exchange_name, sym, r["timestamp"], r["rate"])
                            for r in rates
                        ]
                        await upsert_funding_rates(rows)
                        filled_count += len(rates)
                except Exception as e:
                    logger.debug(
                        f"[{exchange_name}] {sym} backfill failed: {e}"
                    )
                await asyncio.sleep(0.3)

        tasks = [
            asyncio.create_task(_fill_one(s, r)) for s, r in to_fill
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        if filled_count:
            backfilled[exchange_name] = filled_count
            logger.info(
                f"[{exchange_name}] sparse backfill: "
                f"{filled_count} rates for {len(to_fill)} symbols"
            )

    return backfilled


async def deep_integrity_check(
    window_days: int | None = None,
    fix: bool = True,
) -> dict:
    """Per-exchange gap detection over a time window.

    1. Check per-exchange staleness
    2. Scan 2h buckets for missing data → fill from loris
    3. Find symbols with sparse data → backfill from exchange API

    Returns report dict.
    """
    global _last_report

    if window_days is None:
        window_days = DEEP_INTEGRITY_WINDOW_DAYS

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - window_days * 86_400_000

    # 1. Per-exchange staleness
    exchange_latest = await get_per_exchange_latest_ts()
    stale_exchanges = {}
    threshold_ms = STALENESS_THRESHOLD_HOURS * 3_600_000

    for exchange, max_ts in exchange_latest.items():
        age_ms = now_ms - max_ts
        if age_ms > threshold_ms:
            stale_exchanges[exchange] = round(age_ms / 3_600_000, 1)

    # 2. Bucket gap scan per exchange
    first_bucket = start_ms // BUCKET_MS
    last_bucket = now_ms // BUCKET_MS

    gaps_by_exchange: dict[str, list[tuple[int, int]]] = {}
    gap_summary: dict[str, list[dict]] = {}

    for exchange in exchange_latest:
        bucket_counts = await get_exchange_bucket_counts(
            exchange, start_ms, now_ms, BUCKET_MS
        )
        gaps = _find_gaps_in_buckets(bucket_counts, first_bucket, last_bucket)
        if gaps:
            gaps_by_exchange[exchange] = gaps
            gap_summary[exchange] = [
                {
                    "start": datetime.fromtimestamp(
                        s / 1000, tz=timezone.utc
                    ).isoformat(),
                    "end": datetime.fromtimestamp(
                        e / 1000, tz=timezone.utc
                    ).isoformat(),
                    "hours": round((e - s) / 3_600_000, 1),
                }
                for s, e in gaps
            ]

    total_gap_hours = sum(
        sum(g["hours"] for g in gl) for gl in gap_summary.values()
    )

    logger.info(
        f"Deep integrity: {len(exchange_latest)} exchanges, "
        f"{len(stale_exchanges)} stale, "
        f"{len(gaps_by_exchange)} with gaps "
        f"({total_gap_hours:.1f}h total)"
    )

    # 3. Fill bucket gaps from loris
    filled: dict[str, int] = {}
    if gaps_by_exchange:
        for ex, gl in gaps_by_exchange.items():
            total_h = sum((e - s) / 3_600_000 for s, e in gl)
            logger.info(f"  Gap: {ex} — {len(gl)} gap(s), {total_h:.1f}h")

    if fix and gaps_by_exchange:
        # Filter to loris-available exchanges early
        loris_exs = await _get_loris_exchanges()
        loris_gaps = {
            ex: gaps for ex, gaps in gaps_by_exchange.items()
            if ex in loris_exs
        }
        non_loris = set(gaps_by_exchange) - set(loris_gaps)
        if non_loris:
            logger.info(
                f"Skipping {len(non_loris)} exchange(s) not on loris: "
                f"{', '.join(sorted(non_loris))}"
            )

        if loris_gaps:
            symbols = await get_all_symbols()
            if symbols:
                logger.info(
                    f"Filling gaps for {len(loris_gaps)} exchanges "
                    f"across {len(symbols)} symbols..."
                )
                filled = await _fill_gaps_from_loris(loris_gaps, symbols)
                total_filled = sum(filled.values())
                logger.info(f"Deep integrity fill done: {total_filled:,} records")

    # 3b. Thin bucket detection: buckets with data but < 30% of median density
    #     (server was off but loris had partial data → 1 symbol instead of 500)
    thin_buckets = await get_thin_buckets(start_ms, now_ms)
    thin_filled = 0

    if thin_buckets:
        logger.info(
            f"Thin buckets: {len(thin_buckets)} "
            f"(< 30% of median density)"
        )
        if fix:
            try:
                symbols
            except NameError:
                symbols = await get_all_symbols()
            if symbols:
                thin_filled = await _fill_thin_buckets_from_loris(
                    thin_buckets, symbols
                )
                if thin_filled:
                    logger.info(
                        f"Thin bucket fill: {thin_filled:,} records"
                    )

    # 4. Coverage comparison: find (exchange, symbol) pairs underfilled vs peers
    symbol_counts = await get_symbol_point_counts(start_ms, now_ms)
    underfilled = _find_underfilled_symbols(symbol_counts)
    underfilled_total = sum(len(s) for s in underfilled.values())
    coverage_filled: dict[str, int] = {}

    if underfilled:
        logger.info(
            f"Underfilled: {underfilled_total} (exchange,symbol) pairs "
            f"across {len(underfilled)} exchanges"
        )
        if fix:
            coverage_filled = await _fill_underfilled_from_loris(
                underfilled, start_ms, now_ms
            )
            total_cf = sum(coverage_filled.values())
            if total_cf:
                logger.info(f"Coverage fill from loris: {total_cf:,} records")

    # 4b. Per-symbol hourly coverage gaps — detects holes within otherwise
    #     dense data that exchange-level and peer-comparison checks miss.
    #     E.g. variational has data for 500 symbols but SENT is missing 31h.
    hourly_gaps = await get_coverage_gaps(start_ms, now_ms)
    hourly_gap_filled: dict[str, int] = {}

    if hourly_gaps:
        # Group by exchange for loris fill
        gap_by_ex: dict[str, list[str]] = {}
        for ex, sym, filled_h, span_h in hourly_gaps:
            if ex not in gap_by_ex:
                gap_by_ex[ex] = []
            gap_by_ex[ex].append(sym)

        total_gap_pairs = sum(len(s) for s in gap_by_ex.values())
        logger.info(
            f"Per-symbol coverage gaps: {total_gap_pairs} (exchange,symbol) "
            f"pairs with < 85% hourly coverage"
        )

        if fix:
            hourly_gap_filled = await _fill_underfilled_from_loris(
                gap_by_ex, start_ms, now_ms
            )
            total_hgf = sum(hourly_gap_filled.values())
            if total_hgf:
                logger.info(
                    f"Per-symbol coverage fill from loris: {total_hgf:,} records"
                )

    # 5. Backfill sparse symbols from exchange APIs (< 10 points total)
    sparse_filled: dict[str, int] = {}
    if fix:
        sparse_filled = await _backfill_sparse_symbols(
            exchange_latest, start_ms, now_ms
        )

    if fix and (coverage_filled or sparse_filled or thin_filled or hourly_gap_filled):
        await rebuild_current_rates()

    report = {
        "status": "ok",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "window_days": window_days,
        "exchanges_checked": len(exchange_latest),
        "stale_exchanges": stale_exchanges,
        "gaps": gap_summary,
        "total_gap_hours": round(total_gap_hours, 1),
        "filled": filled,
        "thin_buckets": len(thin_buckets),
        "thin_filled": thin_filled,
        "underfilled": {ex: len(syms) for ex, syms in underfilled.items()},
        "coverage_filled": coverage_filled,
        "hourly_gap_pairs": len(hourly_gaps),
        "hourly_gap_filled": hourly_gap_filled,
        "sparse_backfilled": sparse_filled,
        "fix_applied": fix and (
            bool(gaps_by_exchange) or bool(coverage_filled)
            or bool(sparse_filled) or bool(thin_filled)
            or bool(hourly_gap_filled)
        ),
    }

    _last_report = report
    return report


async def fill_coverage_gaps(window_days: int = 7) -> dict[str, int]:
    """Fast targeted gap fill: find symbols with < 85% hourly coverage, download from loris.

    Much faster than deep_integrity_check because it ONLY downloads symbols
    that actually have gaps (typically 100-300 symbols vs 1700+ total).

    Returns {exchange: rows_inserted}.
    """
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - window_days * 86_400_000

    # 1. Find (exchange, symbol) pairs with coverage gaps
    hourly_gaps = await get_coverage_gaps(start_ms, now_ms)
    if not hourly_gaps:
        logger.info("No coverage gaps found — all data looks good")
        return {}

    # 2. Group by exchange for targeted fill
    gap_by_ex: dict[str, list[str]] = {}
    for ex, sym, filled_h, span_h in hourly_gaps:
        if ex not in gap_by_ex:
            gap_by_ex[ex] = []
        gap_by_ex[ex].append(sym)

    total_pairs = sum(len(s) for s in gap_by_ex.values())
    unique_syms = set()
    for syms in gap_by_ex.values():
        unique_syms.update(syms)

    logger.info(
        f"Coverage gaps: {total_pairs} (exchange,symbol) pairs, "
        f"{len(unique_syms)} unique symbols, {len(gap_by_ex)} exchanges"
    )

    # 3. Download from loris
    filled = await _fill_underfilled_from_loris(gap_by_ex, start_ms, now_ms)
    total = sum(filled.values())
    if total:
        await rebuild_current_rates()
        logger.info(f"Coverage gap fill done: {total:,} records inserted")
    else:
        logger.info("Loris returned no new data for gap symbols")

    return filled


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Data integrity check")
    parser.add_argument("--check", action="store_true", help="Run deep integrity check")
    parser.add_argument("--fill-gaps", action="store_true",
                        help="Fast: only fill per-symbol coverage gaps from loris")
    parser.add_argument("--fix", action="store_true", help="Fill detected gaps from loris")
    parser.add_argument("--days", type=int, default=7, help="Window in days (default: 7)")
    args = parser.parse_args()

    if args.fill_gaps:
        from database import init_db

        async def _fill():
            await init_db()
            filled = await fill_coverage_gaps(window_days=args.days)
            if filled:
                print(f"\nFilled from loris:")
                for ex, cnt in sorted(filled.items(), key=lambda x: -x[1]):
                    if cnt > 0:
                        print(f"  {ex}: {cnt:,} rows")
                print(f"  TOTAL: {sum(filled.values()):,} rows")
            else:
                print("No gaps filled (loris had no additional data)")

        asyncio.run(_fill())
    elif args.check:
        from database import init_db

        async def _run():
            await init_db()
            report = await deep_integrity_check(
                window_days=args.days, fix=args.fix
            )
            print(f"\n{'='*60}")
            print(f"Deep Integrity Report -- last {args.days} days")
            print(f"{'='*60}")
            print(f"Exchanges checked: {report['exchanges_checked']}")
            print(f"Total gap hours:   {report['total_gap_hours']}")

            if report["stale_exchanges"]:
                print(f"\nStale exchanges ({len(report['stale_exchanges'])}):")
                for ex, hours in sorted(
                    report["stale_exchanges"].items(), key=lambda x: -x[1]
                ):
                    print(f"  {ex}: {hours}h behind")

            if report["gaps"]:
                print(f"\nGaps ({len(report['gaps'])} exchanges):")
                for ex, gl in sorted(report["gaps"].items()):
                    total_h = sum(g["hours"] for g in gl)
                    print(f"  {ex}: {len(gl)} gap(s), {total_h:.1f}h total")
                    for g in gl[:5]:
                        print(f"    {g['start']} -> {g['end']} ({g['hours']}h)")
                    if len(gl) > 5:
                        print(f"    ... and {len(gl) - 5} more")
            else:
                print("\nNo gaps detected.")

            if report["filled"]:
                print(f"\nLoris gap fill:")
                for ex, cnt in sorted(
                    report["filled"].items(), key=lambda x: -x[1]
                ):
                    if cnt > 0:
                        print(f"  {ex}: {cnt:,} rows")

            if report.get("thin_buckets"):
                print(f"\nThin buckets: {report['thin_buckets']} "
                      f"(< 30% of median density)")
                if report.get("thin_filled"):
                    print(f"  Filled: {report['thin_filled']:,} rows from loris")

            if report.get("underfilled"):
                print(f"\nUnderfilled (< 50% of best exchange):")
                for ex, cnt in sorted(
                    report["underfilled"].items(), key=lambda x: -x[1]
                ):
                    print(f"  {ex}: {cnt} symbol(s)")

            if report.get("coverage_filled"):
                print(f"\nCoverage fill from loris:")
                for ex, cnt in sorted(
                    report["coverage_filled"].items(),
                    key=lambda x: -x[1],
                ):
                    if cnt > 0:
                        print(f"  {ex}: {cnt:,} rows")

            if report.get("hourly_gap_pairs"):
                print(f"\nPer-symbol coverage gaps: "
                      f"{report['hourly_gap_pairs']} (exchange,symbol) pairs")
                if report.get("hourly_gap_filled"):
                    print(f"  Filled from loris:")
                    for ex, cnt in sorted(
                        report["hourly_gap_filled"].items(),
                        key=lambda x: -x[1],
                    ):
                        if cnt > 0:
                            print(f"    {ex}: {cnt:,} rows")

            if report.get("sparse_backfilled"):
                print(f"\nSparse symbol backfill (from exchange APIs):")
                for ex, cnt in sorted(
                    report["sparse_backfilled"].items(),
                    key=lambda x: -x[1],
                ):
                    if cnt > 0:
                        print(f"  {ex}: {cnt:,} rates")

        asyncio.run(_run())
    else:
        parser.print_help()
