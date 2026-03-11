import json as _json

from .base import BaseExchange


class HTX(BaseExchange):
    name = "htx"
    native_interval_hours = 8

    async def _get_hbdm(self, url: str, params: dict | None = None) -> dict:
        """HTX derivatives API returns text/plain, need custom parsing."""
        session = await self._get_session()
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            text = await resp.text()
            return _json.loads(text)

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get_hbdm(
            "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info"
        )
        symbols = []
        for s in data.get("data", []):
            raw = s.get("contract_code", "")
            if raw.endswith("-USDT") and s.get("contract_status") == 1:
                normalized = raw.split("-")[0]
                symbols.append((normalized, raw))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get_hbdm(
            "https://api.hbdm.com/linear-swap-api/v1/swap_batch_funding_rate"
        )
        rates = []
        for item in data.get("data", []):
            raw = item.get("contract_code", "")
            if not raw.endswith("-USDT"):
                continue
            sym = raw.split("-")[0]
            fr = item.get("estimated_rate") or item.get("funding_rate")
            if fr is None:
                continue
            rates.append((sym, self._to_bps(float(fr))))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = "https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate"
        all_rates = []
        page = 1
        while True:
            params = {
                "contract_code": raw_symbol,
                "page_index": str(page),
                "page_size": "50",
            }
            data = await self._get_hbdm(url, params=params)
            items = data.get("data", {}).get("data", [])
            if not items:
                break
            for item in items:
                ts = int(item["funding_time"])
                if ts < start_ms or ts > end_ms:
                    continue
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(float(item["funding_rate"])),
                })
            total_page = data.get("data", {}).get("total_page", 1)
            oldest = min(int(i["funding_time"]) for i in items)
            if page >= total_page or oldest <= start_ms:
                break
            page += 1
        return all_rates
