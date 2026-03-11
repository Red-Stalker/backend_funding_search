"""
Generic DEX exchange implementations for smaller/newer protocols.
These use best-guess API patterns and gracefully return empty data if unavailable.
As APIs are discovered, these can be replaced with full implementations.
"""

from .base import BaseExchange


class _GenericDex(BaseExchange):
    """Base for smaller DEXes with unknown/unstable APIs.
    Override base_url, symbol_path, funding_path, and parsing logic as needed.
    """
    base_url: str = ""
    native_interval_hours = 1
    normalize_to_8h = True

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        if not self.base_url:
            return []
        for path in ["/v1/markets", "/api/v1/markets", "/markets", "/api/markets"]:
            try:
                data = await self._get(f"{self.base_url}{path}")
                return self._parse_symbols(data)
            except Exception:
                continue
        return []

    def _parse_symbols(self, data) -> list[tuple[str, str]]:
        symbols = []
        items = data if isinstance(data, list) else data.get("data", data.get("markets", data.get("results", [])))
        if not isinstance(items, list):
            return []
        for m in items:
            if isinstance(m, dict):
                raw = m.get("symbol", m.get("market", m.get("name", "")))
                base = m.get("baseAsset", m.get("base", m.get("baseCurrency", "")))
                if not base and raw:
                    base = raw.split("-")[0].split("_")[0].replace("USDT", "").replace("USD", "").replace("PERP", "").strip("-_")
                if base and len(base) <= 10:
                    symbols.append((base.upper(), raw))
        return symbols

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        if not self.base_url:
            return []
        for path in ["/v1/funding-rates", "/api/v1/funding-rates", "/funding-rate-history", "/api/funding/history"]:
            try:
                params = {"symbol": raw_symbol, "startTime": str(start_ms), "endTime": str(end_ms)}
                data = await self._get(f"{self.base_url}{path}", params=params)
                return self._parse_funding(data, start_ms, end_ms)
            except Exception:
                continue
        return []

    def _parse_funding(self, data, start_ms: int, end_ms: int) -> list[dict]:
        rates = []
        items = data if isinstance(data, list) else data.get("data", data.get("results", data.get("rows", [])))
        if not isinstance(items, list):
            return []
        for item in items:
            if not isinstance(item, dict):
                continue
            ts = 0
            for k in ["time", "timestamp", "ts", "fundingTime", "created_at", "t"]:
                if k in item:
                    ts = int(item[k])
                    break
            rate = 0.0
            for k in ["fundingRate", "funding_rate", "rate", "r", "value"]:
                if k in item:
                    rate = float(item[k])
                    break
            if ts and start_ms <= ts <= end_ms:
                rates.append({"timestamp": ts, "rate": self._to_bps(rate)})
        return rates


class EdgeX(_GenericDex):
    name = "edgex"
    base_url = "https://api.edgex.exchange"


class Felix(_GenericDex):
    name = "felix"
    base_url = "https://api.felix.exchange"


class Hyena(_GenericDex):
    name = "hyena"
    base_url = "https://api.hyena.finance"


class Kinetiq(_GenericDex):
    name = "kinetiq"
    base_url = "https://api.kinetiq.finance"


class TradeXYZ(_GenericDex):
    name = "tradexyz"
    base_url = "https://api.trade.xyz"
