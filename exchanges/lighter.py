from .base import BaseExchange


class Lighter(BaseExchange):
    """Lighter - ZK rollup perp DEX at mainnet.zklighter.elliot.ai"""
    name = "lighter"
    native_interval_hours = 1
    normalize_to_8h = True

    _BASE = "https://mainnet.zklighter.elliot.ai"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get(f"{self._BASE}/api/v1/orderBooks", params={"filter": "perp"})
        symbols = []
        for m in data.get("order_books", []):
            if m.get("market_type") == "perp" and m.get("status") == "active":
                symbol = m.get("symbol", "")
                market_id = m.get("market_id")
                if symbol and market_id is not None:
                    symbols.append((symbol, str(market_id)))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get(f"{self._BASE}/api/v1/funding-rates")
        rates = []
        for item in data.get("funding_rates", []):
            if item.get("exchange") != "lighter":
                continue
            symbol = item.get("symbol", "")
            rate = float(item.get("rate", 0))
            if symbol:
                rates.append((symbol, self._to_bps(-rate)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = f"{self._BASE}/api/v1/fundings"
        all_rates = []
        start_s = start_ms // 1000
        end_s = end_ms // 1000
        try:
            params = {
                "market_id": int(raw_symbol),
                "resolution": "1h",
                "start_timestamp": start_s,
                "end_timestamp": end_s,
                "count_back": 500,
            }
            data = await self._get(url, params=params)
            for item in data.get("fundings", []):
                ts = int(item.get("timestamp", 0)) * 1000  # API returns seconds
                if ts < start_ms or ts > end_ms:
                    continue
                # rate is percentage string like "0.0020" meaning 0.002%
                rate_pct = float(item.get("rate", 0))
                rate_decimal = -rate_pct / 100
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(rate_decimal),
                })
        except Exception:
            pass
        return all_rates
