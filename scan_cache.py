"""Pre-computed scan cache.

After each snapshot collection, recompute scan results for standard
time ranges (1d, 7d, 30d) and keep in memory. User requests are served
instantly with zero SQL overhead.

For non-standard / custom ranges, falls back to on-demand computation
with a short TTL cache.
"""
import logging
import time
import asyncio
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# Standard ranges that get pre-computed (covers ~95% of user requests)
STANDARD_RANGES = {
    "1d": 1,
    "7d": 7,
    "30d": 30,
}

# Pre-computed results: {range_key: {"tickers": [...], "computed_at": float}}
_precomputed: dict[str, dict] = {}

# On-demand cache for custom ranges: {cache_key: (expire_ts, result)}
_ondemand_cache: dict[str, tuple[float, dict]] = {}
ONDEMAND_TTL = 120  # 2 min

# Lock to prevent concurrent recomputation
_recompute_lock = asyncio.Lock()


async def recompute_standard_scans():
    """Recompute scan results for all standard ranges. Called after snapshot collection."""
    from database import get_bulk_scan, get_all_current_rates

    async with _recompute_lock:
        t0 = time.time()

        current = await get_all_current_rates()
        exchange_list = list(current["funding_rates"].keys())
        if not exchange_list:
            logger.warning("scan_cache: no exchanges found, skipping recompute")
            return

        interval_map = {ex: 1 if ex == "drift" else 8 for ex in exchange_list}
        now = datetime.now(timezone.utc)

        for label, days in STANDARD_RANGES.items():
            try:
                start_dt = now - timedelta(days=days)
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(now.timestamp() * 1000)

                tickers = await get_bulk_scan(exchange_list, start_ms, end_ms, interval_map)
                _precomputed[label] = {
                    "tickers": tickers,
                    "exchanges": sorted(exchange_list),
                    "computed_at": time.time(),
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
                logger.info(f"scan_cache: {label} recomputed — {len(tickers)} tickers")
            except Exception as e:
                logger.error(f"scan_cache: failed to recompute {label}: {e}")

        elapsed = time.time() - t0
        logger.info(f"scan_cache: all ranges recomputed in {elapsed:.1f}s")


def get_precomputed(range_label: str, exchanges: list[str] | None = None) -> dict | None:
    """Get pre-computed result for a standard range.
    Returns None if not available or exchanges don't match.
    """
    if range_label not in _precomputed:
        return None

    cached = _precomputed[range_label]

    # If specific exchanges requested, check they match
    if exchanges is not None:
        if sorted(exchanges) != cached["exchanges"]:
            return None

    # Stale if older than 90 minutes (snapshot is every 60min)
    if time.time() - cached["computed_at"] > 5400:
        return None

    return {"tickers": cached["tickers"]}


def match_standard_range(start_ms: int, end_ms: int) -> str | None:
    """Check if the requested time range matches a standard range (within 30min tolerance)."""
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    span_ms = end_ms - start_ms
    tolerance = 30 * 60_000  # 30 minutes

    # end must be near "now"
    if abs(end_ms - now_ms) > tolerance:
        return None

    for label, days in STANDARD_RANGES.items():
        expected_span = days * 86_400_000
        if abs(span_ms - expected_span) < tolerance:
            return label

    return None


def get_ondemand(cache_key: str) -> dict | None:
    """Get on-demand cached result."""
    if cache_key in _ondemand_cache:
        expire_ts, result = _ondemand_cache[cache_key]
        if time.time() < expire_ts:
            return result
        del _ondemand_cache[cache_key]
    return None


def set_ondemand(cache_key: str, result: dict):
    """Store on-demand result with TTL."""
    now = time.time()
    _ondemand_cache[cache_key] = (now + ONDEMAND_TTL, result)
    # Evict expired
    expired = [k for k, (exp, _) in _ondemand_cache.items() if now >= exp]
    for k in expired:
        del _ondemand_cache[k]
