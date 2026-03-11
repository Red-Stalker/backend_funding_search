from .base import BaseExchange


class Reya(BaseExchange):
    """Reya Network - api.reya.xyz/v2
    No historical funding endpoint found; collects current rate snapshots."""
    name = "reya"
    native_interval_hours = 1  # API fundingRate is % per hour
    normalize_to_8h = True

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api.reya.xyz/v2/marketDefinitions")
        symbols = []
        if isinstance(data, list):
            for m in data:
                raw = m.get("symbol", "")
                if raw.endswith("RUSDPERP"):
                    base = raw.replace("RUSDPERP", "")
                    symbols.append((base, raw))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get("https://api.reya.xyz/v2/markets/summary")
        rates = []
        if isinstance(data, list):
            for m in data:
                raw = m.get("symbol", "")
                if raw.endswith("RUSDPERP"):
                    sym = raw.replace("RUSDPERP", "")
                    fr = float(m.get("fundingRate", 0)) / 100  # % → decimal
                    rates.append((sym, self._to_bps(fr)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        """Fetch current funding rate from market summary (no history endpoint available)."""
        all_rates = []
        try:
            data = await self._get("https://api.reya.xyz/v2/markets/summary")
            if isinstance(data, list):
                for m in data:
                    if m.get("symbol") == raw_symbol:
                        ts = int(m.get("updatedAt", 0))
                        if start_ms <= ts <= end_ms:
                            rate = float(m.get("fundingRate", 0)) / 100  # % → decimal
                            all_rates.append({
                                "timestamp": ts,
                                "rate": self._to_bps(rate),
                            })
                        break
        except Exception:
            pass
        return all_rates
