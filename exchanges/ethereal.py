from .base import BaseExchange


class Ethereal(BaseExchange):
    """Ethereal DEX - api.ethereal.trade"""
    name = "ethereal"
    native_interval_hours = 1
    normalize_to_8h = True

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(
            "https://api.ethereal.trade/v1/product",
            params={"order": "asc", "orderBy": "createdAt"},
        )
        symbols = []
        for m in data.get("data", []):
            if m.get("status") != "ACTIVE":
                continue
            base = m.get("baseTokenName", "")
            product_id = m.get("id", "")
            if base and product_id:
                symbols.append((base, product_id))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get(
            "https://api.ethereal.trade/v1/product",
            params={"order": "asc", "orderBy": "createdAt"},
        )
        rates = []
        for m in data.get("data", []):
            if m.get("status") != "ACTIVE":
                continue
            base = m.get("baseTokenName", "")
            fr = m.get("currentFundingRate1h", m.get("fundingRate1h", None))
            if base and fr is not None:
                rates.append((base, self._to_bps(float(fr))))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = "https://api.ethereal.trade/v1/funding"
        all_rates = []
        # Try MONTH first (up to 50 results), then WEEK, DAY
        for range_val in ["MONTH", "WEEK", "DAY"]:
            try:
                params = {"productId": raw_symbol, "range": range_val}
                data = await self._get(url, params=params)
                for item in data.get("data", []):
                    ts = int(item.get("createdAt", 0))
                    if ts < start_ms or ts > end_ms:
                        continue
                    rate = float(item.get("fundingRate1h", 0))
                    all_rates.append({
                        "timestamp": ts,
                        "rate": self._to_bps(rate),
                    })
                if all_rates:
                    break
            except Exception:
                continue
        return all_rates
