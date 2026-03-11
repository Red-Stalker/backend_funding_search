from .base import BaseExchange


class Phemex(BaseExchange):
    name = "phemex"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api.phemex.com/public/products")
        symbols = []
        for s in data.get("data", {}).get("perpProductsV2", []):
            raw = s.get("symbol", "")  # e.g. "BTCUSDT"
            if raw.endswith("USDT") and s.get("status") == "Listed":
                normalized = raw[: -len("USDT")]
                symbols.append((normalized, raw))
                # fundingInterval in seconds
                fi = s.get("fundingInterval")
                if fi:
                    self._funding_intervals[raw] = int(fi) // 3600
        return symbols

    async def _load_funding_intervals(self):
        try:
            data = await self._get("https://api.phemex.com/public/products")
            for s in data.get("data", {}).get("perpProductsV2", []):
                raw = s.get("symbol", "")
                fi = s.get("fundingInterval")
                if fi:
                    self._funding_intervals[raw] = int(fi) // 3600
        except Exception:
            pass

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        if not self._funding_intervals:
            await self._load_funding_intervals()
        data = await self._get("https://api.phemex.com/md/v3/ticker/24hr/all")
        rates = []
        for item in data.get("result", []):
            raw = item.get("symbol", "")
            if not raw.endswith("USDT"):
                continue
            sym = raw[:-4]
            fr = item.get("predFundingRateRr") or item.get("fundingRateRr")
            if fr is not None:
                ih = self._funding_intervals.get(raw, 8)
                rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    @staticmethod
    def _to_fr_symbol(raw_symbol: str) -> str:
        """Convert BTCUSDT -> .BTCUSDTFR8H for funding rate history API."""
        return f".{raw_symbol}FR8H"

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        ih = self._funding_intervals.get(raw_symbol, 8)
        url = "https://api.phemex.com/api-data/public/data/funding-rate-history"
        fr_symbol = self._to_fr_symbol(raw_symbol)
        all_rates = []
        offset = 0
        limit = 200
        while True:
            params = {
                "symbol": fr_symbol,
                "limit": str(limit),
                "offset": str(offset),
            }
            try:
                data = await self._get(url, params=params)
            except Exception:
                break
            items = data.get("data", {}).get("rows", [])
            if not items:
                break
            for item in items:
                ts = int(item.get("fundingTime", 0))
                if ts < start_ms or ts > end_ms:
                    continue
                rate_decimal = float(item.get("fundingRate", 0))
                # Phemex rates may be scaled by 1e8
                if abs(rate_decimal) > 1:
                    rate_decimal = rate_decimal / 1e8
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(rate_decimal, ih),
                })
            # Stop if oldest item is before start or page not full
            oldest = min(int(i.get("fundingTime", 0)) for i in items if i.get("fundingTime"))
            if oldest <= start_ms or len(items) < limit:
                break
            offset += limit
        return all_rates
