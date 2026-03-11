from .base import BaseExchange


class Paradex(BaseExchange):
    """Paradex on StarkNet - api.prod.paradex.trade
    Continuous funding every ~5s. We sample one point per 8h slot.
    Deep backfill is impractical (millions of API calls), so we limit to recent data."""
    name = "paradex"
    native_interval_hours = 8
    normalize_to_8h = False  # API provides funding_rate_8h already

    _SLOT_MS = 8 * 3600 * 1000

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api.prod.paradex.trade/v1/markets")
        symbols = []
        for m in data.get("results", []):
            raw = m.get("symbol", "")
            if raw.endswith("-USD-PERP"):
                base = raw.split("-")[0]
                symbols.append((base, raw))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        """Concurrent per-market funding rate fetch (no true batch endpoint)."""
        import asyncio
        symbols = await self.fetch_symbols()
        if not symbols:
            return []

        rates = []
        sem = asyncio.Semaphore(10)

        async def _fetch_one(base, raw_sym):
            async with sem:
                try:
                    data = await self._get(
                        "https://api.prod.paradex.trade/v1/funding/data",
                        params={"market": raw_sym, "page_size": "1"},
                    )
                    items = data.get("results", [])
                    if items:
                        fr = items[0].get("funding_rate_8h")
                        if fr is not None:
                            rates.append((base, self._to_bps(float(fr))))
                except Exception:
                    pass

        tasks = [asyncio.create_task(_fetch_one(b, r)) for b, r in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = "https://api.prod.paradex.trade/v1/funding/data"
        all_rates = []
        cursor = None
        seen_slots = set()
        # Limit total API calls to avoid infinite pagination
        max_pages = 200  # ~200 pages * 100 items * 5s = ~28 hours of data
        pages = 0
        while pages < max_pages:
            params = {
                "market": raw_symbol,
                "page_size": "100",
            }
            if cursor:
                params["cursor"] = cursor
            try:
                data = await self._get(url, params=params)
            except Exception:
                break
            items = data.get("results", [])
            if not items:
                break
            for item in items:
                ts = int(item.get("created_at", 0))
                if ts < start_ms or ts > end_ms:
                    continue
                slot = ts // self._SLOT_MS
                if slot in seen_slots:
                    continue
                seen_slots.add(slot)
                rate = float(item.get("funding_rate_8h", item.get("funding_rate", 0)))
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(rate),
                })
            cursor = data.get("next")
            if not cursor:
                break
            oldest = min(int(i.get("created_at", 0)) for i in items)
            if oldest <= start_ms:
                break
            pages += 1
        return all_rates
