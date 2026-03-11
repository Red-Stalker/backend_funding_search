import asyncio

from .base import BaseExchange


class GRVT(BaseExchange):
    """GRVT (Gravity) - POST API at market-data.grvt.io"""
    name = "grvt"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._post(
            "https://market-data.grvt.io/full/v1/instruments",
            json_data={},
        )
        symbols = []
        for m in data.get("result", []):
            inst = m.get("instrument", "")
            kind = m.get("kind", "")
            if kind == "PERPETUAL" and "_USDT_" in inst:
                base = m.get("base", inst.split("_")[0])
                symbols.append((base, inst))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        """Batch via concurrent individual ticker calls."""
        symbols = await self.fetch_symbols()
        if not symbols:
            return []

        rates = []
        sem = asyncio.Semaphore(10)

        async def _fetch_ticker(base, raw_sym):
            async with sem:
                try:
                    data = await self._post(
                        "https://market-data.grvt.io/full/v1/ticker",
                        json_data={"instrument": raw_sym},
                    )
                    result = data.get("result", {})
                    fr = result.get("funding_rate")
                    if fr is not None:
                        # GRVT returns rate as percentage (0.0011 = 0.0011%)
                        rates.append((base, self._to_bps(float(fr) / 100)))
                except Exception:
                    pass

        tasks = [asyncio.create_task(_fetch_ticker(b, r)) for b, r in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        all_rates = []
        try:
            data = await self._post(
                "https://market-data.grvt.io/full/v1/funding",
                json_data={
                    "instrument": raw_symbol,
                    "limit": 500,
                    "end_time": str(end_ms * 10**6),  # nanoseconds
                },
            )
            for item in data.get("result", []):
                ts_ns = int(item.get("funding_time", 0))
                ts_ms = ts_ns // 10**6 if ts_ns > 10**15 else ts_ns
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                rate = float(item.get("funding_rate", 0))
                # GRVT returns rate as percentage (e.g. 0.0011 = 0.0011%)
                rate_decimal = rate / 100
                all_rates.append({
                    "timestamp": ts_ms,
                    "rate": self._to_bps(rate_decimal),
                })
        except Exception:
            pass
        return all_rates
