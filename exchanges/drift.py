from .base import BaseExchange


# Well-known Drift perp market indices (stable on-chain)
_DRIFT_MARKETS = {
    0: "SOL", 1: "BTC", 2: "ETH", 3: "APT", 4: "MATIC",
    5: "ARB", 6: "DOGE", 7: "BNB", 8: "SUI", 9: "1MPEPE",
    10: "OP", 11: "RENDER", 12: "XRP", 13: "HNT", 14: "INJ",
    15: "LINK", 16: "RLB", 17: "PYTH", 18: "TIA", 19: "JTO",
    20: "SEI", 21: "WIF", 22: "JUP", 23: "DYM", 24: "TAO",
    25: "W", 26: "TNSR", 27: "KMNO", 28: "BONK", 29: "DRIFT",
}


class Drift(BaseExchange):
    """Drift on Solana - uses data.api.drift.trade"""
    name = "drift"
    native_interval_hours = 1
    normalize_to_8h = False  # Frontend expects drift as 1h BPS

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        symbols = []
        # Probe each known index to confirm it has data
        for idx, base in _DRIFT_MARKETS.items():
            try:
                data = await self._get(
                    "https://data.api.drift.trade/fundingRates",
                    params={"marketIndex": str(idx), "limit": "1"},
                )
                if data.get("fundingRates"):
                    symbols.append((base, str(idx)))
            except Exception:
                continue
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        rates = []
        for idx, base in _DRIFT_MARKETS.items():
            try:
                data = await self._get(
                    "https://data.api.drift.trade/fundingRates",
                    params={"marketIndex": str(idx), "limit": "1"},
                )
                items = data.get("fundingRates", [])
                if items:
                    item = items[0]
                    rate_raw = float(item.get("fundingRate", 0))
                    oracle_raw = float(item.get("oraclePriceTwap", 1))
                    if oracle_raw != 0:
                        rate_decimal = (rate_raw / 1e9) / (oracle_raw / 1e6)
                        rates.append((base, self._to_bps(rate_decimal)))
            except Exception:
                continue
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        market_index = raw_symbol  # raw_symbol is the market index as string
        url = "https://data.api.drift.trade/fundingRates"
        all_rates = []
        try:
            params = {"marketIndex": market_index}
            data = await self._get(url, params=params)
            for item in data.get("fundingRates", []):
                ts = int(item.get("ts", 0)) * 1000  # API returns seconds
                if ts < start_ms or ts > end_ms:
                    continue
                rate_raw = float(item.get("fundingRate", 0))
                oracle_raw = float(item.get("oraclePriceTwap", 1))
                # rate = (fundingRate / FUNDING_RATE_PRECISION) / (oracle / PRICE_PRECISION)
                # FUNDING_RATE_PRECISION = 1e9, PRICE_PRECISION = 1e6
                if oracle_raw == 0:
                    continue
                rate_decimal = (rate_raw / 1e9) / (oracle_raw / 1e6)
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(rate_decimal),
                })
        except Exception:
            pass
        return all_rates
