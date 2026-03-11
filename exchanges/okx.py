from .base import BaseExchange


class OKX(BaseExchange):
    name = "okx"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(
            "https://www.okx.com/api/v5/public/instruments",
            params={"instType": "SWAP"},
        )
        symbols = []
        for s in data.get("data", []):
            inst_id = s["instId"]
            if inst_id.endswith("-USDT-SWAP"):
                base = inst_id.split("-")[0]
                symbols.append((base, inst_id))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        import asyncio
        symbols = await self.fetch_symbols()
        if not symbols:
            return []

        rates = []
        sem = asyncio.Semaphore(10)

        async def _fetch_one(base, inst_id):
            async with sem:
                try:
                    data = await self._get(
                        "https://www.okx.com/api/v5/public/funding-rate",
                        params={"instId": inst_id},
                    )
                    items = data.get("data", [])
                    if items:
                        fr = items[0].get("fundingRate")
                        if fr is not None:
                            # Calculate interval from fundingTime/nextFundingTime
                            ft = int(items[0].get("fundingTime", 0))
                            nft = int(items[0].get("nextFundingTime", 0))
                            ih = round((nft - ft) / 3_600_000) if ft and nft and nft > ft else 8
                            self._funding_intervals[inst_id] = ih
                            rates.append((base, self._to_bps(float(fr), ih)))
                except Exception:
                    pass

        tasks = [asyncio.create_task(_fetch_one(b, r)) for b, r in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        ih = self._funding_intervals.get(raw_symbol, 8)
        url = "https://www.okx.com/api/v5/public/funding-rate-history"
        all_rates = []
        current_after = str(end_ms)
        while True:
            params = {
                "instId": raw_symbol,
                "before": str(start_ms - 1),
                "after": current_after,
                "limit": "100",
            }
            data = await self._get(url, params=params)
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                all_rates.append({
                    "timestamp": int(item["fundingTime"]),
                    "rate": self._to_bps(float(item["fundingRate"]), ih),
                })
            if len(items) < 100:
                break
            oldest_ts = min(int(i["fundingTime"]) for i in items)
            if oldest_ts <= start_ms:
                break
            current_after = str(oldest_ts)
        return all_rates
