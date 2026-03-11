from .binance import Binance
from .bybit import Bybit
from .okx import OKX
from .bitget import Bitget
from .gateio import GateIO
from .htx import HTX
from .kucoin import KuCoin
from .mexc import MEXC
from .hyperliquid import Hyperliquid
from .bingx import BingX
from .phemex import Phemex
from .cryptocom import CryptoCom
from .drift import Drift
from .bluefin import Bluefin
from .paradex import Paradex
from .woofipro import WoofiPro
from .vest import Vest
from .grvt import GRVT
from .lighter import Lighter
from .reya import Reya
from .aster import Aster
from .ethereal import Ethereal
from .extended import Extended
from .nado import Nado
from .pacifica import Pacifica
from .variational import Variational
from .hibachi import Hibachi
from .edgex import EdgeX
from .zeroone import ZeroOne
from .base import BaseExchange

# Dead/unreachable exchanges (removed):
# Felix, Hyena, Kinetiq, TradeXYZ — domains unreachable as of 2026-03
# ZeroOne (01exchange) — zo-mainnet.n1.xyz returning 502 as of 2026-03-10

_EXCHANGE_CLASSES: list[type[BaseExchange]] = [
    Binance, Bybit, OKX, Bitget, GateIO, HTX, KuCoin, MEXC,
    Hyperliquid, BingX, Phemex, CryptoCom,
    Drift, Bluefin, Paradex, WoofiPro, Vest, GRVT, Lighter, Reya,
    Aster, EdgeX, Ethereal, Extended,
    Hibachi, Nado, Pacifica,
    Variational,
]

_instances: dict[str, BaseExchange] = {}


def get_all_exchanges() -> dict[str, BaseExchange]:
    if not _instances:
        for cls in _EXCHANGE_CLASSES:
            inst = cls()
            _instances[inst.name] = inst
    return _instances


def get_exchange(name: str) -> BaseExchange | None:
    return get_all_exchanges().get(name)


async def close_all():
    for ex in _instances.values():
        await ex.close()
