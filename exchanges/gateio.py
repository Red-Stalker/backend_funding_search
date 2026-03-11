from .base import BaseExchange


class GateIO(BaseExchange):
    name = "gateio"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        symbols = []
        for s in data:
            raw = s.get("name", "")  # e.g. "BTC_USDT"
            if raw.endswith("_USDT") and s.get("in_delisting") is not True:
                normalized = raw.split("_")[0]
                symbols.append((normalized, raw))
                # funding_interval is in seconds
                fi = s.get("funding_interval")
                if fi:
                    self._funding_intervals[raw] = int(fi) // 3600
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        data = await self._get("https://api.gateio.ws/api/v4/futures/usdt/contracts")
        rates = []
        for item in data:
            raw = item.get("name", "")
            if not raw.endswith("_USDT"):
                continue
            sym = raw.split("_")[0]
            fr = item.get("funding_rate_indicative", "0")
            # funding_interval in seconds
            fi = item.get("funding_interval")
            ih = int(fi) // 3600 if fi else self._funding_intervals.get(raw, 8)
            rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        ih = self._funding_intervals.get(raw_symbol, 8)
        url = "https://api.gateio.ws/api/v4/futures/usdt/funding_rate"
        all_rates = []
        limit = 1000
        # Gate.io uses unix timestamps in seconds, not ms
        current_start = start_ms // 1000
        end_sec = end_ms // 1000
        while current_start < end_sec:
            params = {
                "contract": raw_symbol,
                "from": str(current_start),
                "to": str(end_sec),
                "limit": str(limit),
            }
            data = await self._get(url, params=params)
            if not data:
                break
            for item in data:
                ts_sec = int(item["t"])
                all_rates.append({
                    "timestamp": ts_sec * 1000,
                    "rate": self._to_bps(float(item["r"]), ih),
                })
            if len(data) < limit:
                break
            current_start = max(int(i["t"]) for i in data) + 1
        return all_rates
