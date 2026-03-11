from .base import BaseExchange


class Extended(BaseExchange):
    name = "extended"
    native_interval_hours = 1
    normalize_to_8h = True

    _BASE = "https://api.starknet.extended.exchange/api/v1"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        try:
            data = await self._get(f"{self._BASE}/info/markets")
            symbols = []
            for m in data.get("data", []):
                raw = m.get("name", "")  # e.g. "BTC-USD"
                if not m.get("active", False):
                    continue
                base = m.get("assetName", "")
                if not base and "-" in raw:
                    base = raw.split("-")[0]
                if base and raw:
                    symbols.append((base.upper(), raw))
            return symbols
        except Exception:
            return []

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        try:
            data = await self._get(f"{self._BASE}/info/markets")
            rates = []
            for m in data.get("data", []):
                if not m.get("active", False):
                    continue
                base = m.get("assetName", "")
                raw = m.get("name", "")
                if not base and "-" in raw:
                    base = raw.split("-")[0]
                # fundingRate is inside marketStats, not top-level
                stats = m.get("marketStats", {})
                fr = stats.get("fundingRate", None) if stats else None
                if base and fr is not None:
                    rates.append((base.upper(), self._to_bps(float(fr))))
            return rates
        except Exception:
            return []

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = f"{self._BASE}/info/{raw_symbol}/funding"
        all_rates = []
        try:
            params = {"startTime": str(start_ms), "endTime": str(end_ms)}
            data = await self._get(url, params=params)
            for item in data.get("data", []):
                ts = int(item.get("T", 0))
                rate = float(item.get("f", 0))
                if start_ms <= ts <= end_ms:
                    all_rates.append({
                        "timestamp": ts,
                        "rate": self._to_bps(rate),
                    })
        except Exception:
            pass
        return all_rates
