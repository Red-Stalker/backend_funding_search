from .base import BaseExchange


class Pacifica(BaseExchange):
    """Pacifica - api.pacifica.fi/api/v1
    1h funding interval, cursor-based pagination, max 4000 per request."""
    name = "pacifica"
    native_interval_hours = 1
    normalize_to_8h = True

    _BASE = "https://api.pacifica.fi/api/v1"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        try:
            data = await self._get(f"{self._BASE}/info")
            symbols = []
            for m in data.get("data", []):
                raw = m.get("symbol", "")  # e.g. "BTC", "ETH"
                if raw:
                    symbols.append((raw.upper(), raw))
            return symbols
        except Exception:
            return []

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        try:
            data = await self._get(f"{self._BASE}/info")
            rates = []
            for m in data.get("data", []):
                sym = m.get("symbol", "")
                fr = m.get("fundingRate", m.get("funding_rate", None))
                if sym and fr is not None:
                    rates.append((sym.upper(), self._to_bps(float(fr))))
            return rates
        except Exception:
            return []

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = f"{self._BASE}/funding_rate/history"
        all_rates = []
        cursor = None

        for _ in range(100):  # max 100 pages
            params = {"symbol": raw_symbol, "limit": "4000"}
            if cursor:
                params["cursor"] = cursor

            try:
                data = await self._get(url, params=params)
            except Exception:
                break

            items = data.get("data", [])
            if not items:
                break

            for item in items:
                ts = int(item.get("created_at", 0))
                rate = float(item.get("funding_rate", 0))
                if start_ms <= ts <= end_ms:
                    all_rates.append({
                        "timestamp": ts,
                        "rate": self._to_bps(rate),
                    })

            # Check if oldest record is before our start
            oldest_ts = min(int(i["created_at"]) for i in items)
            if oldest_ts <= start_ms:
                break

            if not data.get("has_more", False):
                break

            cursor = data.get("next_cursor")
            if not cursor:
                break

        return all_rates
