import aiosqlite
from config import DB_PATH


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    return db


async def init_db():
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS funding_rates (
                exchange TEXT NOT NULL,
                symbol   TEXT NOT NULL,
                ts       INTEGER NOT NULL,
                rate     REAL NOT NULL,
                PRIMARY KEY (exchange, symbol, ts)
            ) WITHOUT ROWID;

            CREATE INDEX IF NOT EXISTS idx_fr_symbol_ts
                ON funding_rates(symbol, ts);

            CREATE INDEX IF NOT EXISTS idx_fr_exchange_ts
                ON funding_rates(exchange, ts);

            CREATE TABLE IF NOT EXISTS current_rates (
                exchange TEXT NOT NULL,
                symbol   TEXT NOT NULL,
                rate     REAL NOT NULL,
                ts       INTEGER NOT NULL,
                PRIMARY KEY (exchange, symbol)
            ) WITHOUT ROWID;

            CREATE TABLE IF NOT EXISTS exchange_symbols (
                exchange   TEXT NOT NULL,
                symbol     TEXT NOT NULL,
                raw_symbol TEXT NOT NULL,
                PRIMARY KEY (exchange, symbol)
            ) WITHOUT ROWID;
        """)
        await db.commit()
    finally:
        await db.close()


async def upsert_funding_rates(rows: list[tuple]):
    """Insert funding rates, ignoring duplicates.
    rows: list of (exchange, symbol, ts_ms, rate_bps)
    """
    if not rows:
        return
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR IGNORE INTO funding_rates (exchange, symbol, ts, rate) VALUES (?, ?, ?, ?)",
            rows,
        )
        await db.commit()
    finally:
        await db.close()


async def upsert_current_rates(rows: list[tuple]):
    """Update current rates snapshot.
    rows: list of (exchange, symbol, rate_bps, ts_ms)
    """
    if not rows:
        return
    db = await get_db()
    try:
        await db.executemany(
            """INSERT INTO current_rates (exchange, symbol, rate, ts)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(exchange, symbol) DO UPDATE SET rate=excluded.rate, ts=excluded.ts""",
            rows,
        )
        await db.commit()
    finally:
        await db.close()


async def upsert_exchange_symbols(rows: list[tuple]):
    """rows: list of (exchange, symbol, raw_symbol)"""
    if not rows:
        return
    db = await get_db()
    try:
        await db.executemany(
            """INSERT INTO exchange_symbols (exchange, symbol, raw_symbol)
               VALUES (?, ?, ?)
               ON CONFLICT(exchange, symbol) DO UPDATE SET raw_symbol=excluded.raw_symbol""",
            rows,
        )
        await db.commit()
    finally:
        await db.close()


async def get_all_current_rates() -> dict:
    """Returns {symbols: [...], funding_rates: {exchange: {symbol: rate}}}"""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT exchange, symbol, rate FROM current_rates")
        rows = await cursor.fetchall()

        symbols_set = set()
        funding_rates: dict[str, dict[str, float]] = {}
        for row in rows:
            ex, sym, rate = row["exchange"], row["symbol"], row["rate"]
            symbols_set.add(sym)
            if ex not in funding_rates:
                funding_rates[ex] = {}
            funding_rates[ex][sym] = rate

        return {
            "symbols": sorted(symbols_set),
            "funding_rates": funding_rates,
        }
    finally:
        await db.close()


async def get_historical_rates(symbol: str, exchanges: list[str], start_ms: int, end_ms: int,
                               interval_hours_map: dict[str, int] | None = None) -> dict:
    """Returns {series: {exchange: [{t: ISO, y: rate_bps}, ...]}, settlements: {exchange: [ts_ms, ...]}}
    Bucket size by range: 1h for <=7d, 4h for 14d/30d.
    Forward-fills gaps up to 9h (covers 8h settlement intervals).
    Includes settlement timestamps for chart markers.
    """
    from datetime import datetime, timezone

    span_days = (end_ms - start_ms) / 86_400_000
    if span_days <= 8:
        bucket_ms = 60 * 60_000      # 1h buckets for 1d/7d
    else:
        bucket_ms = 4 * 60 * 60_000  # 4h buckets for 14d/30d
    max_gap_ms = 9 * 3_600_000  # forward-fill up to 9h (covers 8h settlement gaps)

    db = await get_db()
    try:
        placeholders = ",".join("?" for _ in exchanges)
        cursor = await db.execute(
            f"""SELECT exchange,
                       (ts / {bucket_ms}) * {bucket_ms} AS ts,
                       AVG(rate) AS rate
                FROM funding_rates
                WHERE symbol = ? AND exchange IN ({placeholders})
                AND ts >= ? AND ts <= ?
                GROUP BY exchange, ts / {bucket_ms}
                ORDER BY ts ASC""",
            [symbol, *exchanges, start_ms, end_ms],
        )
        rows = await cursor.fetchall()

        # Fetch real settlement timestamps for markers
        settlement_cursor = await db.execute(
            f"""SELECT exchange, ts FROM funding_rates
                WHERE symbol = ? AND exchange IN ({placeholders})
                AND ts >= ? AND ts <= ?
                ORDER BY ts ASC""",
            [symbol, *exchanges, start_ms, end_ms],
        )
        settlement_rows = await settlement_cursor.fetchall()
    finally:
        await db.close()

    # Build per-exchange lookup
    exchange_data: dict[str, dict[int, float]] = {}
    for row in rows:
        ex = row["exchange"]
        ts = row["ts"]
        if ex not in exchange_data:
            exchange_data[ex] = {}
        exchange_data[ex][ts] = row["rate"]

    # Group settlement timestamps by exchange
    settlements: dict[str, list[int]] = {}
    for row in settlement_rows:
        ex = row["exchange"]
        if ex not in settlements:
            settlements[ex] = []
        settlements[ex].append(row["ts"])

    # Generate regular time grid
    grid_start = (start_ms // bucket_ms) * bucket_ms
    grid = range(grid_start, end_ms + 1, bucket_ms)

    # Forward-fill each exchange across the regular grid
    series: dict[str, list] = {}
    for ex in exchange_data:
        series[ex] = []
        last_rate = None
        last_real_ts = None

        for ts in grid:
            if ts in exchange_data[ex]:
                last_rate = exchange_data[ex][ts]
                last_real_ts = ts
            elif last_rate is not None and (ts - last_real_ts) <= max_gap_ms:
                pass  # forward-fill with last known rate
            else:
                continue  # no prior data or gap too large

            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            series[ex].append({
                "t": dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "y": round(last_rate, 6),
            })

    return {"series": series, "settlements": settlements}


async def get_bulk_scan(exchanges: list[str], start_ms: int, end_ms: int,
                        interval_hours_map: dict[str, int] | None = None) -> list[dict]:
    """Compute funding spread for ALL symbols via SQL aggregation.
    Uses AVG(rate) approximation instead of row-level Riemann sum for speed.
    Returns list of pre-processed ticker dicts ready for the frontend.
    """
    import math

    if not exchanges:
        return []

    default_map = interval_hours_map or {}
    total_hours = (end_ms - start_ms) / 3_600_000

    span_ms = end_ms - start_ms
    # Data must start within the first 15% and end within the last 15% of the window
    edge_margin = span_ms * 0.15
    min_count = max(5, int(total_hours / 16))  # ~1 point per 16h minimum
    peer_ratio = 0.25          # must have >= 25% of best exchange's count per symbol
    min_density = 0.70         # must fill >= 70% of 6h buckets in the window

    db = await get_db()
    try:
        placeholders = ",".join("?" for _ in exchanges)
        bucket_6h = 21_600_000  # 6-hour buckets for density check
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

    # Pass 1: filter by coverage and minimum count, build pre-grouped data
    pre_grouped: dict[str, dict[str, dict]] = {}
    for row in rows:
        sym = row["symbol"]
        ex = row["exchange"]
        cnt = row["cnt"]

        # Filter 1: data must START near the beginning of the window
        if row["min_ts"] > start_ms + edge_margin:
            continue

        # Filter 2: data must END near the end of the window
        if row["max_ts"] < end_ms - edge_margin:
            continue

        # Filter 3: minimum point count based on window size
        if cnt < min_count:
            continue

        # Filter 4: density — must have data in >= 60% of 6h time buckets
        expected_buckets = max(1, span_ms // 21_600_000)
        if row["filled_6h"] / expected_buckets < min_density:
            continue

        interval_h = default_map.get(ex, 8)
        avg_rate = row["avg_rate"]
        total_pct = (avg_rate / interval_h) * total_hours / 100

        scale = 87.6 / interval_h
        variance = row["avg_rate_sq"] * scale * scale - (avg_rate * scale) ** 2
        std_dev = math.sqrt(max(0, variance))

        if sym not in pre_grouped:
            pre_grouped[sym] = {}
        pre_grouped[sym][ex] = {
            "total_pct": total_pct, "stability": std_dev, "cnt": cnt,
        }

    # Pass 2: per-symbol relative filter — drop exchanges with << points vs best peer
    # Normalize cnt by interval_h so 1h and 8h exchanges are comparable
    grouped: dict[str, dict[str, dict]] = {}
    for sym, ex_data in pre_grouped.items():
        if len(ex_data) < 2:
            continue
        # Normalize: a 1h exchange has 8x more points than 8h for same period
        normalized = {
            ex: d["cnt"] * (default_map.get(ex, 8) / 8)
            for ex, d in ex_data.items()
        }
        best_norm = max(normalized.values())
        threshold = best_norm * peer_ratio
        filtered = {
            ex: d for ex, d in ex_data.items() if normalized[ex] >= threshold
        }
        if len(filtered) >= 2:
            grouped[sym] = filtered

    results = []
    for sym, ex_data in grouped.items():
        exchange_totals = {ex: d["total_pct"] for ex, d in ex_data.items()}
        exchange_stability = {ex: d["stability"] for ex, d in ex_data.items()}

        min_ex = min(exchange_totals, key=exchange_totals.get)
        max_ex = max(exchange_totals, key=exchange_totals.get)
        spread = exchange_totals[max_ex] - exchange_totals[min_ex]

        stability = max(
            exchange_stability.get(min_ex, 0),
            exchange_stability.get(max_ex, 0),
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


async def rebuild_current_rates():
    """Rebuild current_rates from funding_rates (latest rate per exchange/symbol)."""
    db = await get_db()
    try:
        await db.execute("""
            INSERT OR REPLACE INTO current_rates (exchange, symbol, rate, ts)
            SELECT f.exchange, f.symbol, f.rate, f.ts
            FROM funding_rates f
            INNER JOIN (
                SELECT exchange, symbol, MAX(ts) as max_ts
                FROM funding_rates GROUP BY exchange, symbol
            ) latest ON f.exchange = latest.exchange
                AND f.symbol = latest.symbol
                AND f.ts = latest.max_ts
        """)
        await db.commit()
    finally:
        await db.close()


async def get_exchange_symbols(exchange: str) -> list[tuple[str, str]]:
    """Returns list of (symbol, raw_symbol) for an exchange."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT symbol, raw_symbol FROM exchange_symbols WHERE exchange = ?",
            [exchange],
        )
        rows = await cursor.fetchall()
        return [(row["symbol"], row["raw_symbol"]) for row in rows]
    finally:
        await db.close()


# ---------- Integrity / coverage queries ----------

async def get_global_latest_ts() -> int | None:
    """Returns the most recent timestamp across all data, or None if DB is empty."""
    db = await get_db()
    try:
        cursor = await db.execute("SELECT MAX(ts) as max_ts FROM funding_rates")
        row = await cursor.fetchone()
        return row["max_ts"] if row and row["max_ts"] else None
    finally:
        await db.close()


async def get_all_symbols() -> list[str]:
    """Returns sorted list of all distinct symbols in funding_rates."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT DISTINCT symbol FROM funding_rates ORDER BY symbol"
        )
        return [row["symbol"] for row in await cursor.fetchall()]
    finally:
        await db.close()


async def get_latest_ts_per_symbol() -> dict[str, int]:
    """Returns {symbol: max_ts} across all exchanges."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT symbol, MAX(ts) as max_ts FROM funding_rates GROUP BY symbol"
        )
        return {row["symbol"]: row["max_ts"] for row in await cursor.fetchall()}
    finally:
        await db.close()


async def get_per_exchange_latest_ts() -> dict[str, int]:
    """Returns {exchange: max_ts} for staleness checking."""
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT exchange, MAX(ts) as max_ts FROM funding_rates GROUP BY exchange"
        )
        return {row["exchange"]: row["max_ts"] for row in await cursor.fetchall()}
    finally:
        await db.close()


async def get_exchange_bucket_counts(
    exchange: str, start_ms: int, end_ms: int, bucket_ms: int = 7_200_000
) -> dict[int, int]:
    """Returns {bucket_id: count} for gap detection.
    bucket_id = ts // bucket_ms. Default 2-hour buckets.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT (ts / ?) AS bucket, COUNT(*) as cnt
               FROM funding_rates
               WHERE exchange = ? AND ts >= ? AND ts <= ?
               GROUP BY ts / ?""",
            [bucket_ms, exchange, start_ms, end_ms, bucket_ms],
        )
        return {row["bucket"]: row["cnt"] for row in await cursor.fetchall()}
    finally:
        await db.close()


async def get_sparse_exchange_symbols(
    exchange: str, start_ms: int, min_points: int = 10
) -> list[str]:
    """Find symbols for an exchange that have fewer than min_points in the window.
    These are symbols with incomplete history that need backfilling.
    Returns list of normalized symbols.
    """
    db = await get_db()
    try:
        # Get all symbols this exchange SHOULD have (from current_rates)
        # then find which ones have too few points in the window
        cursor = await db.execute(
            """SELECT cr.symbol, COALESCE(fr.cnt, 0) as cnt
               FROM current_rates cr
               LEFT JOIN (
                   SELECT symbol, COUNT(*) as cnt
                   FROM funding_rates
                   WHERE exchange = ? AND ts >= ?
                   GROUP BY symbol
               ) fr ON cr.symbol = fr.symbol
               WHERE cr.exchange = ?
               AND COALESCE(fr.cnt, 0) < ?""",
            [exchange, start_ms, exchange, min_points],
        )
        return [row["symbol"] for row in await cursor.fetchall()]
    finally:
        await db.close()


async def get_thin_buckets(
    start_ms: int, end_ms: int, bucket_ms: int = 7_200_000, threshold_ratio: float = 0.3,
) -> list[tuple[int, int]]:
    """Find time buckets where data density is abnormally low across ALL exchanges.
    Compares each bucket's total point count to the median bucket count.
    Returns [(start_ms, end_ms), ...] for thin buckets.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT (ts / ?) AS bucket, COUNT(*) as cnt
               FROM funding_rates
               WHERE ts >= ? AND ts <= ?
               GROUP BY ts / ?
               ORDER BY bucket""",
            [bucket_ms, start_ms, end_ms, bucket_ms],
        )
        rows = await cursor.fetchall()
    finally:
        await db.close()

    if len(rows) < 3:
        return []

    import statistics
    counts = [r["cnt"] for r in rows]
    median = statistics.median(counts)
    if median < 100:
        return []

    threshold = median * threshold_ratio
    thin = []
    for row in rows:
        if row["cnt"] < threshold:
            b = row["bucket"]
            thin.append((b * bucket_ms, (b + 1) * bucket_ms))
    return thin


async def get_symbol_point_counts(
    start_ms: int, end_ms: int,
) -> dict[str, dict[str, int]]:
    """Returns {symbol: {exchange: count}} for coverage comparison.
    Used to find (exchange, symbol) pairs with significantly fewer points
    than the best exchange for that symbol.
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT exchange, symbol, COUNT(*) as cnt
               FROM funding_rates
               WHERE ts >= ? AND ts <= ?
               GROUP BY exchange, symbol""",
            [start_ms, end_ms],
        )
        result: dict[str, dict[str, int]] = {}
        for row in await cursor.fetchall():
            sym = row["symbol"]
            if sym not in result:
                result[sym] = {}
            result[sym][row["exchange"]] = row["cnt"]
        return result
    finally:
        await db.close()


async def get_coverage_gaps(
    start_ms: int, end_ms: int,
    coverage_threshold: float = 0.85,
    min_span_hours: int = 48,
) -> list[tuple[str, str, int, int]]:
    """Find (exchange, symbol) pairs with hourly coverage below threshold.
    Detects per-symbol gaps that exchange-level checks miss.
    Returns [(exchange, symbol, filled_hours, span_hours), ...].
    """
    db = await get_db()
    try:
        cursor = await db.execute(
            """SELECT exchange, symbol,
                      COUNT(DISTINCT ts / 3600000) as filled_hours,
                      CAST((MAX(ts) - MIN(ts)) / 3600000 AS INTEGER) + 1 as span_hours
               FROM funding_rates
               WHERE ts >= ? AND ts <= ?
               GROUP BY exchange, symbol
               HAVING span_hours >= ?
               AND CAST(filled_hours AS REAL) / span_hours < ?""",
            [start_ms, end_ms, min_span_hours, coverage_threshold],
        )
        return [
            (row["exchange"], row["symbol"], row["filled_hours"], row["span_hours"])
            for row in await cursor.fetchall()
        ]
    finally:
        await db.close()


async def get_data_coverage() -> list[dict]:
    """Get data coverage statistics per exchange.
    Returns list of dicts with: exchange, oldest_ts, newest_ts, symbols_count, total_points.
    """
    db = await get_db()
    try:
        cursor = await db.execute("""
            SELECT exchange,
                   MIN(ts) as oldest_ts,
                   MAX(ts) as newest_ts,
                   COUNT(DISTINCT symbol) as symbols_count,
                   COUNT(*) as total_points
            FROM funding_rates
            GROUP BY exchange
        """)
        rows = await cursor.fetchall()
        return [
            {
                "exchange": row["exchange"],
                "oldest_ts": row["oldest_ts"],
                "newest_ts": row["newest_ts"],
                "symbols_count": row["symbols_count"],
                "total_points": row["total_points"],
            }
            for row in rows
        ]
    finally:
        await db.close()


async def get_db_stats() -> dict:
    """Get overall database statistics."""
    db = await get_db()
    try:
        cur = await db.execute("SELECT COUNT(*) as cnt FROM funding_rates")
        total = (await cur.fetchone())["cnt"]

        cur = await db.execute("SELECT COUNT(DISTINCT exchange) as cnt FROM funding_rates")
        exchanges = (await cur.fetchone())["cnt"]

        cur = await db.execute("SELECT COUNT(DISTINCT symbol) as cnt FROM funding_rates")
        symbols = (await cur.fetchone())["cnt"]

        cur = await db.execute("SELECT MIN(ts) as min_ts, MAX(ts) as max_ts FROM funding_rates")
        row = await cur.fetchone()

        return {
            "total_records": total,
            "exchanges": exchanges,
            "symbols": symbols,
            "oldest_ts": row["min_ts"],
            "newest_ts": row["max_ts"],
        }
    finally:
        await db.close()
