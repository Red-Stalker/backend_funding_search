"""Loris.tools API client with proxy rotation and retry logic."""
import asyncio
import logging
import random
from datetime import datetime

import aiohttp

from proxies import get_proxy

logger = logging.getLogger(__name__)

LORIS_BASE = "https://loris.tools/api"

# Mapping: loris exchange name -> our exchange name
LORIS_TO_OUR = {
    "huobi": "htx",
}


class LorisClient:
    """Client for loris.tools funding rate API."""

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60),
                connector=connector,
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _fetch(self, url: str, params: dict,
                     max_retries: int = 5) -> dict | None:
        """Fetch URL with proxy rotation, exponential backoff, and retry."""
        session = await self._get_session()
        for attempt in range(max_retries):
            proxy = get_proxy()
            kwargs: dict = {"params": params}
            if proxy:
                kwargs["proxy"] = proxy
            try:
                async with session.get(url, **kwargs) as resp:
                    if resp.status == 429:
                        wait = min(2 ** attempt + random.random() * 2, 30)
                        logger.debug(f"429 rate-limited, waiting {wait:.1f}s")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        await asyncio.sleep(1 + random.random())
                        continue
                    return await resp.json()
            except Exception as e:
                wait = min(1 + 2 ** attempt * 0.5 + random.random(), 15)
                logger.debug(
                    f"Loris request failed (attempt {attempt + 1}): {e}"
                )
                await asyncio.sleep(wait)
        logger.warning(f"Loris fetch failed after {max_retries} attempts: {url}")
        return None

    def _map_exchange(self, loris_name: str) -> str:
        """Map loris exchange name to our name."""
        return LORIS_TO_OUR.get(loris_name, loris_name)

    async def fetch_symbols(self) -> list[str]:
        """Fetch list of all symbols from loris."""
        data = await self._fetch(f"{LORIS_BASE}/funding", {})
        if data and "symbols" in data:
            return data["symbols"]
        return []

    async def fetch_current_rates(self) -> dict[str, dict[str, float]]:
        """Fetch current rates for all exchanges.
        Returns {our_exchange_name: {symbol: rate_bps}}.
        """
        data = await self._fetch(f"{LORIS_BASE}/funding", {})
        if not data or "funding_rates" not in data:
            return {}
        result = {}
        for exchange, rates in data["funding_rates"].items():
            our_name = self._map_exchange(exchange)
            result[our_name] = rates
        return result

    async def fetch_historical(
        self, symbol: str, start: datetime, end: datetime
    ) -> dict[str, list[dict]]:
        """Fetch historical funding data for a symbol.
        Returns {our_exchange_name: [{t: ISO, y: bps}, ...]}.
        """
        params = {
            "symbol": symbol,
            "start": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        }
        data = await self._fetch(
            f"{LORIS_BASE}/funding/historical", params
        )
        if not data or "series" not in data:
            return {}

        result = {}
        for exchange, points in data["series"].items():
            our_name = self._map_exchange(exchange)
            result[our_name] = points
        return result

    async def list_exchanges(self) -> dict[str, int]:
        """Get exchanges and their symbol counts from loris.
        Returns {our_exchange_name: symbol_count}.
        """
        data = await self._fetch(f"{LORIS_BASE}/funding", {})
        if not data or "funding_rates" not in data:
            return {}
        result = {}
        for exchange, rates in data["funding_rates"].items():
            our_name = self._map_exchange(exchange)
            result[our_name] = len(rates)
        return result
