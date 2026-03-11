import logging
import time

import aiohttp

from .base import BaseExchange

logger = logging.getLogger(__name__)


class Nado(BaseExchange):
    """Nado - gateway.prod.nado.xyz / archive.prod.nado.xyz
    1h funding interval. Only current rate available (no public history).
    Rate is funding_rate_x18 / 1e18 = daily rate. Convert to hourly: / 24."""
    name = "nado"
    native_interval_hours = 1
    normalize_to_8h = True

    _GATEWAY = "https://gateway.prod.nado.xyz/v1/query"
    _ARCHIVE = "https://archive.prod.nado.xyz/v1"

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        try:
            data = await self._post(self._GATEWAY, json_data={"type": "symbols"})
            symbols_map = data.get("data", {}).get("symbols", {})
            symbols = []
            for name, info in symbols_map.items():
                if info.get("type") != "perp":
                    continue
                pid = info.get("product_id")
                base = name.replace("-PERP", "")
                if base and pid:
                    symbols.append((base.upper(), str(pid)))
            return symbols
        except Exception:
            return []

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        try:
            # Get all symbols first
            sym_data = await self._post(self._GATEWAY, json_data={"type": "symbols"})
            symbols_map = sym_data.get("data", {}).get("symbols", {})
            rates = []
            session = await self._get_session()
            for name, info in symbols_map.items():
                if info.get("type") != "perp":
                    continue
                pid = info.get("product_id")
                base = name.replace("-PERP", "").upper()
                if not pid:
                    continue
                try:
                    async with session.post(
                        self._ARCHIVE,
                        json={"funding_rate": {"product_id": pid}},
                        headers={"Accept-Encoding": "gzip"},
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json(content_type=None)
                    if isinstance(data, dict) and "product_id" in data:
                        rate_x18 = int(data.get("funding_rate_x18", 0))
                        daily_rate = rate_x18 / 1e18
                        hourly_rate = daily_rate / 24
                        rates.append((base, self._to_bps(hourly_rate)))
                except Exception:
                    continue
            return rates
        except Exception:
            return []

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        """Fetch current funding rate snapshot (no history endpoint available)."""
        try:
            product_id = int(raw_symbol)
            session = await self._get_session()
            async with session.post(
                self._ARCHIVE,
                json={"funding_rate": {"product_id": product_id}},
                headers={"Accept-Encoding": "gzip"},
            ) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)

            if not isinstance(data, dict) or "product_id" not in data:
                return []

            rate_x18 = int(data.get("funding_rate_x18", 0))
            update_time = int(data.get("update_time", 0))
            ts_ms = update_time * 1000

            # daily rate; convert to hourly
            daily_rate = rate_x18 / 1e18
            hourly_rate = daily_rate / 24

            return [{
                "timestamp": ts_ms,
                "rate": self._to_bps(hourly_rate),
            }]
        except Exception as e:
            logger.debug(f"[nado] funding for product {raw_symbol}: {e}")
            return []
