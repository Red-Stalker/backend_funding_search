from .base import BaseExchange


class BingX(BaseExchange):
    name = "bingx"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://open-api.bingx.com/openApi/swap/v2/quote/contracts")
        symbols = []
        for s in data.get("data", []):
            raw = s.get("symbol", "")  # e.g. "BTC-USDT"
            if "-USDT" in raw:
                normalized = raw.split("-")[0]
                symbols.append((normalized, raw))
        # BingX doesn't expose funding intervals — use binance as reference
        await self._load_binance_intervals()
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        if not self._funding_intervals:
            await self._load_binance_intervals()
        data = await self._get(
            "https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex"
        )
        rates = []
        for item in data.get("data", []):
            raw = item.get("symbol", "")
            if "-USDT" not in raw:
                continue
            sym = raw.split("-")[0]
            fr = item.get("lastFundingRate", "0")
            ih = self._funding_intervals.get(sym, 8)
            rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        sym = raw_symbol.split("-")[0]
        ih = self._funding_intervals.get(sym, 8)
        url = "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate"
        all_rates = []
        current_end = end_ms
        while True:
            params = {
                "symbol": raw_symbol,
                "startTime": str(start_ms),
                "endTime": str(current_end),
                "limit": "1000",
            }
            try:
                data = await self._get(url, params=params)
            except Exception:
                break
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                ts = int(item.get("fundingTime", 0))
                rate_str = item.get("fundingRate", "0")
                if ts and rate_str:
                    all_rates.append({
                        "timestamp": ts,
                        "rate": self._to_bps(float(rate_str), ih),
                    })
            if len(items) < 1000:
                break
            oldest = min(int(i.get("fundingTime", 0)) for i in items if i.get("fundingTime"))
            if oldest <= start_ms:
                break
            current_end = oldest - 1
        return all_rates
