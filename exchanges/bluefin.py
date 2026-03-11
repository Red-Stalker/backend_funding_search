import asyncio

from .base import BaseExchange


class Bluefin(BaseExchange):
    name = "bluefin"
    native_interval_hours = 1
    normalize_to_8h = True

    _BASE = "https://api.sui-prod.bluefin.io/v1/exchange"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        try:
            data = await self._get(f"{self._BASE}/info")
            symbols = []
            for m in data.get("markets", []):
                raw = m.get("symbol", "")  # e.g. "BTC-PERP"
                base = m.get("baseAssetSymbol", "")
                if raw and base:
                    symbols.append((base.upper(), raw))
            return symbols
        except Exception:
            return []

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        try:
            # /exchange/tickers has estimatedFundingRateE9 field
            tickers = await self._get(f"{self._BASE}/tickers")
            if not isinstance(tickers, list):
                return []
            rates = []
            for t in tickers:
                sym = t.get("symbol", "")  # e.g. "BTC-PERP"
                if not sym or not sym.endswith("-PERP"):
                    continue
                base = sym.replace("-PERP", "")
                fr_e9 = t.get("estimatedFundingRateE9", t.get("lastFundingRateE9"))
                if fr_e9 is not None:
                    rate_decimal = int(fr_e9) / 1e9
                    rates.append((base.upper(), self._to_bps(rate_decimal)))
            return rates
        except Exception:
            return []

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = f"{self._BASE}/fundingRateHistory"
        all_rates = []
        cursor_end = end_ms

        for _ in range(50):  # max 50 pages
            params = {
                "symbol": raw_symbol,
                "startTime": str(start_ms),
                "endTime": str(cursor_end),
                "limit": "100",
            }
            try:
                data = await self._get(url, params=params)
            except Exception:
                break

            if not isinstance(data, list) or not data:
                break

            for item in data:
                ts = int(item.get("fundingTimeAtMillis", 0))
                rate_e9 = int(item.get("fundingRateE9", 0))
                rate_decimal = rate_e9 / 1e9
                if start_ms <= ts <= end_ms:
                    all_rates.append({
                        "timestamp": ts,
                        "rate": self._to_bps(rate_decimal),
                    })

            if len(data) < 100:
                break

            await asyncio.sleep(0.5)  # rate limit protection

            # Results are reverse chronological; paginate backwards
            oldest_ts = min(int(d["fundingTimeAtMillis"]) for d in data)
            if oldest_ts <= start_ms:
                break
            cursor_end = oldest_ts - 1

        return all_rates
