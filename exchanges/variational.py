import time

from .base import BaseExchange


class Variational(BaseExchange):
    """Variational - omni-client-api.prod.ap-northeast-1.variational.io
    8h funding interval. Current rate snapshot only (no history endpoint)."""
    name = "variational"
    native_interval_hours = 8
    normalize_to_8h = False  # Rate already converted to 8h equivalent

    _BASE = "https://omni-client-api.prod.ap-northeast-1.variational.io"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        try:
            data = await self._get(f"{self._BASE}/metadata/stats")
            symbols = []
            for listing in data.get("listings", []):
                ticker = listing.get("ticker", "")
                if ticker:
                    symbols.append((ticker.upper(), ticker))
            return symbols
        except Exception:
            return []

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get(f"{self._BASE}/metadata/stats")
        rates = []
        for listing in data.get("listings", []):
            ticker = listing.get("ticker", "")
            if ticker:
                rate = float(listing.get("funding_rate", "0")) / 1000  # ‰ → decimal
                rates.append((ticker.upper(), self._to_bps(rate)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        """Fetch current funding rate from stats (no history endpoint)."""
        try:
            data = await self._get(f"{self._BASE}/metadata/stats")
            ts_ms = int(time.time() * 1000)

            for listing in data.get("listings", []):
                if listing.get("ticker") == raw_symbol:
                    rate_str = listing.get("funding_rate", "0")
                    rate = float(rate_str)
                    rate_decimal = rate / 1000  # ‰ → decimal
                    return [{
                        "timestamp": ts_ms,
                        "rate": self._to_bps(rate_decimal),
                    }]
        except Exception:
            pass
        return []
