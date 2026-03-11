import asyncio
import logging
import ssl

import aiohttp

from .base import BaseExchange
from proxies import get_proxy

logger = logging.getLogger(__name__)


class EdgeX(BaseExchange):
    """edgeX - StarkEx perp DEX at pro.edgex.exchange
    Funding interval: 240 min (4h). Rate is decimal (e.g. -0.00001655).
    API is behind Cloudflare — requests go through proxy pool.
    No batch funding endpoint — uses concurrent per-contract calls.
    """
    name = "edgex"
    native_interval_hours = 4
    normalize_to_8h = True

    _BASE = "https://pro.edgex.exchange"
    _HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(resolver=aiohttp.resolver.ThreadedResolver())
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self._HEADERS,
            )
        return self._session

    async def _proxy_get(self, url: str, params: dict | None = None) -> dict:
        """GET via proxy to bypass Cloudflare. Retries with different proxies."""
        session = await self._get_session()
        last_err = None
        for _ in range(3):
            proxy = get_proxy()
            try:
                async with session.get(url, params=params, proxy=proxy, ssl=False) as resp:
                    if resp.status == 403:
                        last_err = Exception(f"403 Forbidden via {proxy}")
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as e:
                last_err = e
                continue
        raise last_err or Exception("All proxy attempts failed")

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._proxy_get(f"{self._BASE}/api/v1/public/meta/getMetaData")
        contracts = data.get("data", {}).get("contractList", [])
        seen = {}
        for c in contracts:
            if not c.get("enableTrade"):
                continue
            cid = str(c.get("contractId", ""))
            name = c.get("contractName", "")  # e.g. "BTCUSD", "BNB2USD"
            # Normalize: strip "USD", then strip trailing digit if preceded by letter
            base = name.replace("USD", "")
            if base and base[-1].isdigit() and len(base) > 1 and base[-2].isalpha():
                base = base[:-1]
            if base and cid:
                key = base.upper()
                if key not in seen:
                    seen[key] = cid
        return list(seen.items())

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        symbols = await self.fetch_symbols()
        if not symbols:
            return []

        rates = []
        sem = asyncio.Semaphore(3)
        failed_streak = 0

        async def _fetch_one(base, contract_id):
            nonlocal failed_streak
            if failed_streak >= 10:
                return
            async with sem:
                try:
                    data = await self._proxy_get(
                        f"{self._BASE}/api/v1/public/funding/getLatestFundingRate",
                        params={"contractId": contract_id},
                    )
                    items = data.get("data", [])
                    if items:
                        fr = items[0].get("forecastFundingRate",
                                          items[0].get("fundingRate"))
                        if fr is not None:
                            rates.append((base, self._to_bps(float(fr))))
                            failed_streak = 0
                except Exception:
                    failed_streak += 1
                await asyncio.sleep(0.3)

        tasks = [asyncio.create_task(_fetch_one(b, cid)) for b, cid in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        all_rates = []
        offset = None

        for _ in range(100):
            params = {
                "contractId": raw_symbol,
                "size": "100",
                "filterSettlementFundingRate": "true",
            }
            if offset:
                params["offsetData"] = offset
            if start_ms:
                params["filterBeginTimeInclusive"] = str(start_ms)
            if end_ms:
                params["filterEndTimeExclusive"] = str(end_ms)

            try:
                data = await self._proxy_get(
                    f"{self._BASE}/api/v1/public/funding/getFundingRatePage",
                    params=params,
                )
                page = data.get("data", {})
                items = page.get("dataList", [])
                if not items:
                    break

                for item in items:
                    ts = int(item.get("fundingTime", 0))
                    rate = float(item.get("fundingRate", 0))
                    if start_ms <= ts <= end_ms:
                        all_rates.append({
                            "timestamp": ts,
                            "rate": self._to_bps(rate),
                        })

                offset = page.get("nextPageOffsetData")
                if not offset:
                    break
            except Exception:
                break

            await asyncio.sleep(0.3)

        return all_rates
