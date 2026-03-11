"""Proxy pool management. Loads proxies from file."""
import os
import random
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_proxies: list[str] = []
_loaded = False


def load_proxies(path: str | None = None) -> list[str]:
    """Load proxy list from file. Returns list of proxy URLs."""
    global _proxies, _loaded
    if _loaded:
        return _proxies

    if path is None:
        path = os.environ.get(
            "PROXY_FILE",
            str(Path(__file__).resolve().parent / "proxies.txt"),
        )

    try:
        with open(path) as f:
            _proxies = [
                line.strip()
                for line in f
                if line.strip() and not line.startswith("#")
            ]
        logger.info(f"Loaded {len(_proxies)} proxies from {path}")
    except FileNotFoundError:
        logger.warning(f"Proxy file not found: {path} — will connect directly")
        _proxies = []

    _loaded = True
    return _proxies


def get_proxy() -> str | None:
    """Get a random proxy from the pool. Returns None if no proxies."""
    proxies = load_proxies()
    return random.choice(proxies) if proxies else None
