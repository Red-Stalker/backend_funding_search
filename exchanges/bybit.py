from .base import BaseExchange


class Bybit(BaseExchange):
    name = "bybit"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "linear", "limit": "1000"},
        )
        symbols = []
        for s in data.get("result", {}).get("list", []):
            if s.get("contractType") == "LinearPerpetual" and s.get("status") == "Trading":
                raw = s["symbol"]
                base = s.get("baseCoin", raw.replace("USDT", ""))
                if raw.endswith("USDT"):
                    symbols.append((base, raw))
                    # fundingInterval is in minutes
                    fi = s.get("fundingInterval")
                    if fi:
                        self._funding_intervals[raw] = int(fi) // 60
        return symbols

    async def _load_funding_intervals(self):
        try:
            data = await self._get(
                "https://api.bybit.com/v5/market/instruments-info",
                params={"category": "linear", "limit": "1000"},
            )
            for s in data.get("result", {}).get("list", []):
                raw = s.get("symbol", "")
                fi = s.get("fundingInterval")
                if fi:
                    self._funding_intervals[raw] = int(fi) // 60
        except Exception:
            pass

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        if not self._funding_intervals:
            await self._load_funding_intervals()
        data = await self._get(
            "https://api.bybit.com/v5/market/tickers",
            params={"category": "linear"},
        )
        rates = []
        for item in data.get("result", {}).get("list", []):
            raw = item.get("symbol", "")
            if not raw.endswith("USDT"):
                continue
            sym = raw[:-4]
            fr = item.get("fundingRate", "0")
            ih = self._funding_intervals.get(raw, 8)
            rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        if not self._funding_intervals:
            await self._load_funding_intervals()
        ih = self._funding_intervals.get(raw_symbol, 8)
        url = "https://api.bybit.com/v5/market/funding/history"
        all_rates = []
        current_end = end_ms
        while True:
            params = {
                "category": "linear",
                "symbol": raw_symbol,
                "startTime": str(start_ms),
                "endTime": str(current_end),
                "limit": "200",
            }
            data = await self._get(url, params=params)
            items = data.get("result", {}).get("list", [])
            if not items:
                break
            for item in items:
                all_rates.append({
                    "timestamp": int(item["fundingRateTimestamp"]),
                    "rate": self._to_bps(float(item["fundingRate"]), ih),
                })
            if len(items) < 200:
                break
            oldest_ts = min(int(i["fundingRateTimestamp"]) for i in items)
            if oldest_ts <= start_ms:
                break
            current_end = oldest_ts - 1
        return all_rates
