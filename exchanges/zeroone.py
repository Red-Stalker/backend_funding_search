import asyncio

from .base import BaseExchange


class ZeroOne(BaseExchange):
    """01 Exchange (01.xyz) — Solana perp DEX.
    API: https://zo-mainnet.n1.xyz — public, no auth, no rate limit.
    Funding: every 1 hour, rate is decimal (e.g. -0.00002).
    24 markets, no batch endpoint — concurrent per-market calls.
    """
    name = "01exchange"
    native_interval_hours = 1
    normalize_to_8h = True

    _BASE = "https://zo-mainnet.n1.xyz"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(f"{self._BASE}/info")
        symbols = []
        for m in data.get("markets", []):
            mid = m.get("marketId")
            sym = m.get("symbol", "")  # e.g. "BTCUSD"
            if mid is not None and sym.endswith("USD"):
                base = sym[:-3]  # strip "USD"
                if base:
                    symbols.append((base.upper(), str(mid)))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        symbols = await self.fetch_symbols()
        if not symbols:
            return []

        rates = []
        sem = asyncio.Semaphore(8)

        async def _fetch_one(base, market_id):
            async with sem:
                try:
                    data = await self._get(
                        f"{self._BASE}/market/{market_id}/stats"
                    )
                    perp = data.get("perpStats", {})
                    fr = perp.get("funding_rate")
                    if fr is not None:
                        rates.append((base, self._to_bps(float(fr))))
                except Exception:
                    pass

        tasks = [asyncio.create_task(_fetch_one(b, mid)) for b, mid in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        """Fetch hourly funding history. API returns last 24 entries only."""
        all_rates = []
        try:
            data = await self._get(
                f"{self._BASE}/market/{raw_symbol}/history/PT1H"
            )
            items = data.get("items", []) if isinstance(data, dict) else data
            if not items:
                return []

            from datetime import datetime, timezone
            for item in items:
                ts_str = item.get("time", "")
                fr = item.get("fundingRate")
                if not ts_str or fr is None:
                    continue
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
                if start_ms <= ts_ms <= end_ms:
                    all_rates.append({
                        "timestamp": ts_ms,
                        "rate": self._to_bps(float(fr)),
                    })
        except Exception:
            pass
        return all_rates
