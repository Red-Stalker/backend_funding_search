from .base import BaseExchange


class MEXC(BaseExchange):
    name = "mexc"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://contract.mexc.com/api/v1/contract/detail")
        symbols = []
        for s in data.get("data", []):
            raw = s.get("symbol", "")  # e.g. "BTC_USDT"
            if raw.endswith("_USDT") and s.get("state") == 0:
                normalized = raw.split("_")[0]
                symbols.append((normalized, raw))
        # MEXC doesn't expose funding intervals — use binance as reference
        await self._load_binance_intervals()
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        if not self._funding_intervals:
            await self._load_binance_intervals()
        data = await self._get("https://contract.mexc.com/api/v1/contract/ticker")
        rates = []
        for item in data.get("data", []):
            raw = item.get("symbol", "")
            if not raw.endswith("_USDT"):
                continue
            sym = raw.split("_")[0]
            fr = item.get("fundingRate", 0)
            if fr is not None:
                ih = self._funding_intervals.get(sym, 8)
                rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        sym = raw_symbol.split("_")[0]
        ih = self._funding_intervals.get(sym, 8)
        url = "https://contract.mexc.com/api/v1/contract/funding_rate/history"
        all_rates = []
        page = 1
        while True:
            params = {
                "symbol": raw_symbol,
                "page_num": str(page),
                "page_size": "100",
            }
            data = await self._get(url, params=params)
            items = data.get("data", {}).get("resultList", [])
            if not items:
                break
            for item in items:
                ts = int(item["settleTime"])
                if ts < start_ms or ts > end_ms:
                    continue
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(float(item["fundingRate"]), ih),
                })
            oldest = min(int(i["settleTime"]) for i in items)
            if oldest <= start_ms or len(items) < 100:
                break
            page += 1
        return all_rates
