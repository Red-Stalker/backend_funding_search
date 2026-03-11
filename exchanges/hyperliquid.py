import asyncio

from .base import BaseExchange


class Hyperliquid(BaseExchange):
    name = "hyperliquid"
    native_interval_hours = 1
    normalize_to_8h = True  # Frontend treats HL as 8h, so normalize

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._post(
            "https://api.hyperliquid.xyz/info",
            json_data={"type": "meta"},
        )
        symbols = []
        for s in data.get("universe", []):
            name = s["name"]  # Already normalized, e.g. "BTC"
            symbols.append((name, name))
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._post(
            "https://api.hyperliquid.xyz/info",
            json_data={"type": "metaAndAssetCtxs"},
        )
        meta = data[0]["universe"]
        ctxs = data[1]
        rates = []
        for m, c in zip(meta, ctxs):
            sym = m["name"]
            fr = float(c.get("funding", 0))
            rates.append((sym, self._to_bps(fr)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        all_rates = []
        current_start = start_ms
        while current_start < end_ms:
            try:
                data = await self._post(
                    "https://api.hyperliquid.xyz/info",
                    json_data={
                        "type": "fundingHistory",
                        "coin": raw_symbol,
                        "startTime": current_start,
                    },
                )
            except Exception:
                # On rate limit, wait and retry once
                await asyncio.sleep(5)
                try:
                    data = await self._post(
                        "https://api.hyperliquid.xyz/info",
                        json_data={
                            "type": "fundingHistory",
                            "coin": raw_symbol,
                            "startTime": current_start,
                        },
                    )
                except Exception:
                    break
            if not data:
                break
            for item in data:
                ts = int(item["time"])
                if ts > end_ms:
                    continue
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(float(item["fundingRate"])),
                })
            if len(data) < 500:
                break
            current_start = max(int(i["time"]) for i in data) + 1
        return all_rates
