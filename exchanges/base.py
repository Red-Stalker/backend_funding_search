import asyncio
import logging
from abc import ABC, abstractmethod

import aiohttp

logger = logging.getLogger(__name__)


class BaseExchange(ABC):
    name: str = ""
    # Native funding interval in hours. Used for normalization.
    native_interval_hours: int = 8
    # Whether to normalize rates to 8h BPS in output.
    # True for all except drift (which frontend expects as 1h).
    normalize_to_8h: bool = True

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None
        # Per-symbol funding interval in hours: {raw_symbol: hours}
        # Populated by fetch_symbols() on exchanges with mixed intervals.
        self._funding_intervals: dict[str, int] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, url: str, params: dict | None = None, headers: dict | None = None) -> dict | list:
        session = await self._get_session()
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 429:
                    logger.warning(f"[{self.name}] Rate limited on {url}")
                    await asyncio.sleep(2)
                    raise aiohttp.ClientError(f"Rate limited: {resp.status}")
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"[{self.name}] GET {url} failed: {e}")
            raise

    async def _post(self, url: str, json_data: dict | None = None) -> dict | list:
        session = await self._get_session()
        try:
            async with session.post(url, json=json_data) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            logger.error(f"[{self.name}] POST {url} failed: {e}")
            raise

    def _to_bps(self, rate_decimal: float, interval_hours: int | None = None) -> float:
        """Convert decimal funding rate to BPS for the API output.
        If interval_hours is given, normalizes to 8h using that value.
        Otherwise uses class-level native_interval_hours + normalize_to_8h flag.
        """
        bps = rate_decimal * 10000
        if interval_hours is not None:
            if interval_hours != 8:
                bps = bps * (8 / interval_hours)
        elif self.normalize_to_8h and self.native_interval_hours != 8:
            bps = bps * (8 / self.native_interval_hours)
        return round(bps, 6)

    async def _load_binance_intervals(self):
        """Load per-symbol intervals from binance as reference for exchanges without own interval API.
        Maps base symbol (e.g. 'BTC') -> interval hours."""
        try:
            session = await self._get_session()
            async with session.get("https://fapi.binance.com/fapi/v1/fundingInfo") as resp:
                data = await resp.json()
            for item in data:
                raw = item.get("symbol", "")
                if raw.endswith("USDT"):
                    base = raw[:-4]
                    ih = int(item.get("fundingIntervalHours", 8))
                    # Store by base symbol for cross-exchange lookup
                    self._funding_intervals[base] = ih
        except Exception:
            pass

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        """Batch fetch current/predicted funding rates for all symbols.
        Returns list of (normalized_symbol, rate_bps).
        Override with efficient batch endpoint where available.
        Default: returns empty (collector falls back to per-symbol).
        """
        return []

    @abstractmethod
    async def fetch_symbols(self) -> list[tuple[str, str]]:
        """Returns list of (normalized_symbol, raw_symbol).
        normalized_symbol: e.g. "BTC", "ETH"
        raw_symbol: exchange-specific, e.g. "BTCUSDT", "BTC-USDT-SWAP"
        """
        ...

    @abstractmethod
    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        """Returns list of {"timestamp": ms, "rate": bps_value}.
        Rate should already be in the correct BPS format (8h or 1h for drift).
        """
        ...
