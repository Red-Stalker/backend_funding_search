from .base import BaseExchange


class CryptoCom(BaseExchange):
    name = "cryptocom"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api.crypto.com/exchange/v1/public/get-instruments")
        symbols = []
        for s in data.get("result", {}).get("data", []):
            raw = s.get("symbol", "")
            inst_type = s.get("inst_type", "")
            if inst_type == "PERPETUAL_SWAP" and raw.endswith("USD-PERP"):
                normalized = raw.replace("USD-PERP", "")
                if normalized:
                    symbols.append((normalized, raw))
        return symbols

    # No batch funding rate endpoint; falls back to per-symbol

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        url = "https://api.crypto.com/exchange/v1/public/get-valuations"
        all_rates = []
        params = {
            "instrument_name": raw_symbol,
            "valuation_type": "funding_hist",
            "count": "1000",
        }
        if start_ms:
            params["start_ts"] = str(start_ms)
        if end_ms:
            params["end_ts"] = str(end_ms)
        try:
            data = await self._get(url, params=params)
            items = data.get("result", {}).get("data", [])
            for item in items:
                ts = int(item.get("t", 0))
                if ts < start_ms or ts > end_ms:
                    continue
                rate = float(item.get("v", 0))
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(rate),
                })
        except Exception:
            pass
        return all_rates
