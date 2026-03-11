from .base import BaseExchange

# KuCoin uses XBT for BTC
KUCOIN_SYMBOL_MAP = {"XBT": "BTC"}
KUCOIN_REVERSE_MAP = {v: k for k, v in KUCOIN_SYMBOL_MAP.items()}


class KuCoin(BaseExchange):
    name = "kucoin"
    native_interval_hours = 8

    async def fetch_symbols(self) -> list[tuple[str, str]]:
        data = await self._get("https://api-futures.kucoin.com/api/v1/contracts/active")
        symbols = []
        for s in data.get("data", []):
            raw = s.get("symbol", "")  # e.g. "XBTUSDTM"
            base = s.get("baseCurrency", "")
            if raw.endswith("USDTM") and s.get("isInverse") is False:
                normalized = KUCOIN_SYMBOL_MAP.get(base, base)
                symbols.append((normalized, raw))
                # fundingRateGranularity is in milliseconds
                fg = s.get("fundingRateGranularity") or s.get("currentFundingRateGranularity")
                if fg:
                    self._funding_intervals[raw] = int(fg) // 3_600_000
        return symbols

    async def fetch_current_rates_batch(self) -> list[tuple[str, float]]:
        # KuCoin contracts/active returns all contracts with fundingFeeRate
        data = await self._get("https://api-futures.kucoin.com/api/v1/contracts/active")
        rates = []
        for item in data.get("data", []):
            raw = item.get("symbol", "")
            if not raw.endswith("USDTM"):
                continue
            base = item.get("baseCurrency", "")
            sym = KUCOIN_SYMBOL_MAP.get(base, base)
            fr = item.get("predictedFundingFeeRate") or item.get("fundingFeeRate")
            if fr is not None:
                fg = item.get("fundingRateGranularity") or item.get("currentFundingRateGranularity")
                ih = int(fg) // 3_600_000 if fg else self._funding_intervals.get(raw, 8)
                rates.append((sym, self._to_bps(float(fr), ih)))
        return rates

    async def fetch_funding_history(self, raw_symbol: str, start_ms: int, end_ms: int) -> list[dict]:
        ih = self._funding_intervals.get(raw_symbol, 8)
        url = "https://api-futures.kucoin.com/api/v1/contract/funding-rates"
        all_rates = []
        current_start = start_ms
        while current_start < end_ms:
            params = {
                "symbol": raw_symbol,
                "from": str(current_start),
                "to": str(end_ms),
            }
            data = await self._get(url, params=params)
            items = data.get("data", [])
            if not items:
                break
            for item in items:
                ts = int(item["timepoint"])
                all_rates.append({
                    "timestamp": ts,
                    "rate": self._to_bps(float(item["fundingRate"]), ih),
                })
            if len(items) < 100:
                break
            current_start = max(int(i["timepoint"]) for i in items) + 1
        return all_rates
