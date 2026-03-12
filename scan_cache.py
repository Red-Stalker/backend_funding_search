"""Pre-computed scan cache with per-exchange stats.

Strategy: pre-compute per-(exchange, symbol) aggregates via SQL once,
then assemble spreads for ANY exchange combination in pure Python (~1ms).

Pre-computed at startup + after each snapshot collection (hourly).
Covers standard ranges (1d, 7d, 30d). Custom ranges fall back to SQL
with a 2-min TTL cache.
"""
import logging
import math
import time
import asyncio
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

STANDARD_RANGES = {"1d": 1, "7d": 7, "14d": 14, "30d": 30}

# Per-range raw stats: {range_label: RangeData}
_range_data: dict[str, "RangeData"] = {}

# On-demand cache for custom ranges
_ondemand_cache: dict[str, tuple[float, dict]] = {}
ONDEMAND_TTL = 120

_recompute_lock = asyncio.Lock()


class RangeData:
    """Pre-computed per-(exchange, symbol) stats for one time range."""
    __slots__ = ("stats", "computed_at", "total_hours", "span_ms", "start_ms", "end_ms")

    def __init__(self, stats: dict, total_hours: float, span_ms: int,
                 start_ms: int, end_ms: int):
        # stats: {symbol: {exchange: {total_pct, stability, cnt}}}
        self.stats = stats
        self.total_hours = total_hours
        self.span_ms = span_ms
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.computed_at = time.time()


async def _fetch_raw_stats(exchanges: list[str], start_ms: int, end_ms: int,
                           interval_map: dict[str, int]) -> dict:
    """Fetch per-(exchange, symbol) aggregates from DB and apply quality filters.
    Returns {symbol: {exchange: {total_pct, stability, cnt}}}.
    """
    from database import get_db

    span_ms = end_ms - start_ms
    total_hours = span_ms / 3_600_000
    edge_margin = span_ms * 0.15
    min_count = max(5, int(total_hours / 16))
    min_density = 0.70
    peer_ratio = 0.25
    bucket_6h = 21_600_000

    db = await get_db()
    try:
        placeholders = ",".join("?" for _ in exchanges)
        cursor = await db.execute(
            f"""SELECT exchange, symbol,
                       AVG(rate) as avg_rate,
                       AVG(rate * rate) as avg_rate_sq,
                       COUNT(*) as cnt,
                       MIN(ts) as min_ts,
                       MAX(ts) as max_ts,
                       COUNT(DISTINCT ts / {bucket_6h}) as filled_6h
                FROM funding_rates
                WHERE exchange IN ({placeholders})
                AND ts >= ? AND ts <= ?
                GROUP BY exchange, symbol
                HAVING cnt >= 2""",
            [*exchanges, start_ms, end_ms],
        )
        rows = await cursor.fetchall()
    finally:
        await db.close()

    expected_buckets = max(1, span_ms // bucket_6h)

    # Pass 1: quality filter
    pre_grouped: dict[str, dict[str, dict]] = {}
    for row in rows:
        sym, ex, cnt = row["symbol"], row["exchange"], row["cnt"]

        if row["min_ts"] > start_ms + edge_margin:
            continue
        if row["max_ts"] < end_ms - edge_margin:
            continue
        if cnt < min_count:
            continue
        if row["filled_6h"] / expected_buckets < min_density:
            continue

        interval_h = interval_map.get(ex, 8)
        avg_rate = row["avg_rate"]
        total_pct = (avg_rate / interval_h) * total_hours / 100

        scale = 87.6 / interval_h
        variance = row["avg_rate_sq"] * scale * scale - (avg_rate * scale) ** 2
        std_dev = math.sqrt(max(0, variance))

        if sym not in pre_grouped:
            pre_grouped[sym] = {}
        pre_grouped[sym][ex] = {"total_pct": total_pct, "stability": std_dev, "cnt": cnt}

    # Pass 2: peer ratio filter (against ALL exchanges, not just requested)
    # Normalize cnt by interval_h so 1h and 8h exchanges are comparable
    result: dict[str, dict[str, dict]] = {}
    for sym, ex_data in pre_grouped.items():
        if len(ex_data) < 2:
            continue
        normalized = {
            ex: d["cnt"] * (interval_map.get(ex, 8) / 8)
            for ex, d in ex_data.items()
        }
        best_norm = max(normalized.values())
        threshold = best_norm * peer_ratio
        filtered = {ex: d for ex, d in ex_data.items() if normalized[ex] >= threshold}
        if len(filtered) >= 2:
            result[sym] = filtered

    return result


def _assemble_tickers(stats: dict, exchanges: list[str] | None = None) -> list[dict]:
    """Assemble final ticker list from pre-computed stats.
    If exchanges is given, filter to only those exchanges.
    Pure Python, ~1ms for 500 symbols.
    """
    exchange_set = set(exchanges) if exchanges else None
    results = []

    for sym, ex_data in stats.items():
        # Filter to requested exchanges
        if exchange_set:
            filtered = {ex: d for ex, d in ex_data.items() if ex in exchange_set}
        else:
            filtered = ex_data

        if len(filtered) < 2:
            continue

        exchange_totals = {ex: d["total_pct"] for ex, d in filtered.items()}
        min_ex = min(exchange_totals, key=exchange_totals.get)
        max_ex = max(exchange_totals, key=exchange_totals.get)
        spread = exchange_totals[max_ex] - exchange_totals[min_ex]

        stability = max(
            filtered.get(min_ex, {}).get("stability", 0),
            filtered.get(max_ex, {}).get("stability", 0),
        )
        smart_score = spread / stability if stability > 0 else 0

        results.append({
            "symbol": sym,
            "spread": round(spread, 6),
            "minRate": round(exchange_totals[min_ex], 6),
            "maxRate": round(exchange_totals[max_ex], 6),
            "minEx": min_ex,
            "maxEx": max_ex,
            "rates": {k: round(v, 6) for k, v in exchange_totals.items()},
            "stability": round(stability, 6),
            "smartScore": round(smart_score, 6),
        })

    results.sort(key=lambda x: -x["spread"])
    return results


async def recompute_standard_scans():
    """Recompute raw stats for all standard ranges. Called after snapshot collection."""
    from database import get_all_current_rates

    async with _recompute_lock:
        t0 = time.time()

        current = await get_all_current_rates()
        exchange_list = list(current["funding_rates"].keys())
        if not exchange_list:
            logger.warning("scan_cache: no exchanges, skipping recompute")
            return

        from exchanges.registry import get_exchange as _get_ex
        interval_map = {}
        for ex_name in exchange_list:
            ex_inst = _get_ex(ex_name)
            interval_map[ex_name] = ex_inst.native_interval_hours if ex_inst else 8
        now = datetime.now(timezone.utc)

        for label, days in STANDARD_RANGES.items():
            try:
                start_dt = now - timedelta(days=days)
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(now.timestamp() * 1000)
                span_ms = end_ms - start_ms

                stats = await _fetch_raw_stats(exchange_list, start_ms, end_ms, interval_map)
                _range_data[label] = RangeData(
                    stats=stats,
                    total_hours=span_ms / 3_600_000,
                    span_ms=span_ms,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
                sym_count = len(stats)
                logger.info(f"scan_cache: {label} pre-computed — {sym_count} symbols")
            except Exception as e:
                logger.error(f"scan_cache: failed {label}: {e}")

        elapsed = time.time() - t0
        logger.info(f"scan_cache: all ranges done in {elapsed:.1f}s")


def get_precomputed(range_label: str, exchanges: list[str] | None) -> dict | None:
    """Assemble tickers from pre-computed stats for any exchange combination."""
    if range_label not in _range_data:
        return None

    rd = _range_data[range_label]
    if time.time() - rd.computed_at > 5400:  # stale after 90min
        return None

    tickers = _assemble_tickers(rd.stats, exchanges)
    return {"tickers": tickers}


def match_standard_range(start_ms: int, end_ms: int) -> str | None:
    """Check if the requested range matches a standard range (within 30min)."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    span_ms = end_ms - start_ms
    tolerance = 30 * 60_000

    if abs(end_ms - now_ms) > tolerance:
        return None

    for label, days in STANDARD_RANGES.items():
        expected_span = days * 86_400_000
        if abs(span_ms - expected_span) < tolerance:
            return label
    return None


def get_ondemand(cache_key: str) -> dict | None:
    if cache_key in _ondemand_cache:
        expire_ts, result = _ondemand_cache[cache_key]
        if time.time() < expire_ts:
            return result
        del _ondemand_cache[cache_key]
    return None


def set_ondemand(cache_key: str, result: dict):
    now = time.time()
    _ondemand_cache[cache_key] = (now + ONDEMAND_TTL, result)
    expired = [k for k, (exp, _) in _ondemand_cache.items() if now >= exp]
    for k in expired:
        del _ondemand_cache[k]
