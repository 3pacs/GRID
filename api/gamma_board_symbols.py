"""Plain US ticker symbols accepted by the private board; futures use a separate feed."""

import re

SYMBOL_PATTERN = r"^[A-Z]{1,5}$"


def provider_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z]{1,5}", symbol):
        raise ValueError("Unsupported board symbol")
    return "$SPX" if symbol == "SPX" else symbol
