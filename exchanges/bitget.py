from .base import BaseExchange


class Bitget(BaseExchange):
    name = "bitget"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(
            "https://api.bitget.com/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
        )
        symbols = []
        for s in data.get("data", []):
            raw = s.get("symbol", "")
            if raw.endswith("USDT"):
                normalized = raw[: -len("USDT")]
                symbols.append((normalized, raw))
        # Fetch per-symbol intervals from contracts endpoint
        await self._load_funding_intervals()
        return symbols

    async def _load_funding_intervals(self):
        try:
            data = await self._get(
                "https://api.bitget.com/api/v2/mix/market/contracts",
                params={"productType": "USDT-FUTURES"},
            )
            for item in data.get("data", []):
                sym = item.get("symbol", "")
                fi = item.get("fundInterval")
                if fi:
                    self._funding_intervals[sym] = int(fi)
        except Exception:
            pass

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        if not self._funding_intervals:
            await self._load_funding_intervals()
        data = await self._get(
            "https://api.bitget.com/api/v2/mix/market/tickers",
            params={"productType": "USDT-FUTURES"},
        )
        rates = []
        for item in data.get("data", []):
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
        url = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"
        all_rates = []
        page = 1
        while True:
            params = {
                "symbol": raw_symbol,
                "productType": "USDT-FUTURES",
                "pageSize": "100",
                "pageNo": str(page),
            }
            data = await self._get(url, params=params)
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                ts = int(item["fundingTime"])
                if ts < start_ms:
                    continue
                if ts > end_ms:
                    continue
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(float(item["fundingRate"]), ih),
                })
            # If oldest item in this page is before start_ms, stop
            oldest = min(int(i["fundingTime"]) for i in items)
            if oldest <= start_ms or len(items) < 100:
                break
            page += 1
        return all_rates
