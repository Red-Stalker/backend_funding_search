from .base import BaseExchange


class Vest(BaseExchange):
    """Vest Exchange - server-prod.hz.vestmarkets.com
    1h funding interval, oneHrFundingRate field, limit 1000/request."""
    name = "vest"
    native_interval_hours = 1
    normalize_to_8h = True

    _BASE = "https://server-prod.hz.vestmarkets.com/v2"
    _HEADERS = {"xrestservermm": "restserver1"}

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(f"{self._BASE}/exchangeInfo", headers=self._HEADERS)
        symbols = []
        for m in data.get("symbols", []):
            raw = m.get("symbol", "")
            if raw.endswith("-PERP"):
                base = raw.replace("-PERP", "").replace("-USD", "")
                symbols.append((base, raw))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get(
            f"{self._BASE}/ticker/latest", headers=self._HEADERS
        )
        rates = []
        items = data.get("tickers", data.get("data", []))
        if isinstance(items, list):
            for item in items:
                raw = item.get("symbol", "")
                if not raw.endswith("-PERP"):
                    continue
                sym = raw.replace("-PERP", "").replace("-USD", "")
                fr = item.get("oneHrFundingRate")
                if fr is not None:
                    rates.append((sym, self._to_bps(float(fr))))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = f"{self._BASE}/funding/history"
        all_rates = []
        current_end = end_ms

        for _ in range(100):  # max pages
            params = {
                "symbol": raw_symbol,
                "startTime": str(start_ms),
                "endTime": str(current_end),
                "limit": "1000",
                "interval": "1h",
            }
            try:
                data = await self._get(url, params=params, headers=self._HEADERS)
            except Exception:
                break

            items = data if isinstance(data, list) else data.get("data", [])
            if not isinstance(items, list) or not items:
                break

            for item in items:
                ts = int(item.get("time", 0))
                if start_ms <= ts <= end_ms:
                    rate = float(item.get("oneHrFundingRate", 0))
                    all_rates.append({
                        "timestamp": ts,
                        "rate": self._to_bps(rate),
                    })

            if len(items) < 1000:
                break

            oldest_ts = min(int(i.get("time", 0)) for i in items)
            if oldest_ts <= start_ms:
                break
            current_end = oldest_ts - 1

        return all_rates
