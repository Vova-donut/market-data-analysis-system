from __future__ import annotations

from typing import Tuple

def normalize_symbol(sym: str) -> str:
    s = sym.strip().upper()
    s = s.replace(":USDT", "")
    s = s.replace("/", "")
    s = s.replace("-", "")
    return s


def split_base_quote(sym: str) -> Tuple[str, str]:
    s = normalize_symbol(sym)
    if not s.endswith("USDT"):
        return s[:-4], s[-4:]
    return s[:-4], "USDT"


def okx_inst_id(sym: str) -> str:
    b, q = split_base_quote(sym)
    return f"{b}-{q}-SWAP"


