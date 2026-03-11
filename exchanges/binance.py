from .base import BaseExchange


class Binance(BaseExchange):
    name = "binance"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        symbols = []
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
                raw = s["symbol"]
                if raw.endswith("USDT"):
                    normalized = raw[: -len("USDT")]
                    symbols.append((normalized, raw))
        # Populate per-symbol funding intervals
        await self._load_funding_intervals()
        return symbols

    async def _load_funding_intervals(self):
        """Fetch per-symbol funding intervals from fundingInfo endpoint."""
        try:
            data = await self._get("https://fapi.binance.com/fapi/v1/fundingInfo")
            self._funding_intervals = {}
            for item in data:
                sym = item.get("symbol", "")
                ih = item.get("fundingIntervalHours", 8)
                self._funding_intervals[sym] = int(ih)
        except Exception:
            pass

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        if not self._funding_intervals:
            await self._load_funding_intervals()
        data = await self._get("https://fapi.binance.com/fapi/v1/premiumIndex")
        rates = []
        for item in data:
            raw = item.get("symbol", "")
            if not raw.endswith("USDT"):
                continue
            sym = raw[:-4]
            fr = item.get("lastFundingRate")
            if fr is not None:
                ih = self._funding_intervals.get(raw, 8)
                rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        if not self._funding_intervals:
            await self._load_funding_intervals()
        ih = self._funding_intervals.get(raw_symbol, 8)
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        all_rates = []
        current_start = start_ms
        while current_start < end_ms:
            params = {
                "symbol": raw_symbol,
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1000,
            }
            data = await self._get(url, params=params)
            if not data:
                break
            for item in data:
                all_rates.append({
                    "timestamp": int(item["fundingTime"]),
                    "rate": self._to_bps(float(item["fundingRate"]), ih),
                })
            if len(data) < 1000:
                break
            current_start = int(data[-1]["fundingTime"]) + 1
        return all_rates
