from .base import BaseExchange


class WoofiPro(BaseExchange):
    """WOOFi Pro / WOO X - api.woo.org"""
    name = "woofipro"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api.woo.org/v1/public/info")
        symbols = []
        for item in data.get("rows", []):
            raw = item.get("symbol", "")
            if raw.startswith("PERP_") and raw.endswith("_USDT"):
                base = raw.replace("PERP_", "").replace("_USDT", "")
                symbols.append((base, raw))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get("https://api.woo.org/v1/public/funding_rates")
        rates = []
        for item in data.get("rows", []):
            raw = item.get("symbol", "")
            if raw.startswith("PERP_") and raw.endswith("_USDT"):
                sym = raw.replace("PERP_", "").replace("_USDT", "")
                fr = item.get("est_funding_rate", item.get("last_funding_rate", 0))
                rates.append((sym, self._to_bps(float(fr))))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = "https://api.woo.org/v1/public/funding_rate_history"
        all_rates = []
        page = 1
        while True:
            params = {
                "symbol": raw_symbol,
                "page": str(page),
                "size": "100",
            }
            try:
                data = await self._get(url, params=params)
            except Exception:
                break
            items = data.get("rows", [])
            if not items:
                break
            for item in items:
                ts = int(item.get("funding_rate_timestamp", 0))
                if ts < start_ms or ts > end_ms:
                    continue
                rate = float(item.get("funding_rate", 0))
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(rate),
                })
            oldest = min(int(i.get("funding_rate_timestamp", 0)) for i in items)
            if oldest <= start_ms or len(items) < 100:
                break
            page += 1
        return all_rates
