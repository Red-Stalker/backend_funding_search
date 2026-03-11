import asyncio
import time

from .base import BaseExchange


class Hibachi(BaseExchange):
    """Hibachi - data-api.hibachi.xyz
    Current funding rate only (estimatedFundingRate). No history endpoint.
    Symbol format: BTC/USDT-P"""
    name = "hibachi"
    native_interval_hours = 8  # estimatedFundingRate is already 8h rate
    normalize_to_8h = False

    _BASE = "https://data-api.hibachi.xyz"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        try:
            data = await self._get(f"{self._BASE}/market/exchange-info")
            symbols = []
            for c in data.get("futureContracts", []):
                raw = c.get("symbol", "")  # e.g. "BTC/USDT-P"
                if raw.endswith("/USDT-P"):
                    base = raw.replace("/USDT-P", "")
                    symbols.append((base.upper(), raw))
            return symbols
        except Exception:
            return []

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        try:
            data = await self._get(f"{self._BASE}/market/exchange-info")
            rates = []
            for c in data.get("futureContracts", []):
                raw = c.get("symbol", "")
                if not raw.endswith("/USDT-P"):
                    continue
                sym = raw.replace("/USDT-P", "").upper()
                try:
                    px_data = await self._get(
                        f"{self._BASE}/market/data/prices",
                        params={"symbol": raw},
                    )
                    fr_info = px_data.get("fundingRateEstimation", {})
                    fr = fr_info.get("estimatedFundingRate", "0")
                    rates.append((sym, self._to_bps(float(fr))))
                except Exception:
                    continue
            return rates
        except Exception:
            return []

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        """Fetch current estimated funding rate (no history endpoint)."""
        try:
            data = await self._get(
                f"{self._BASE}/market/data/prices",
                params={"symbol": raw_symbol},
            )
            fr = data.get("fundingRateEstimation", {})
            rate_str = fr.get("estimatedFundingRate", "0")
            rate = float(rate_str)
            ts_ms = int(time.time() * 1000)

            return [{
                "timestamp": ts_ms,
                "rate": self._to_bps(rate),
            }]
        except Exception:
            return []
