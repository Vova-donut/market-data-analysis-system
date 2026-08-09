from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, List

from ..config import BINANCE_WS_BASE_URL, BINANCE_WS_SHARDS
from ..utils import normalize_symbol
from ..websocket import _loads, _to_text, _ws_loop

if TYPE_CHECKING:
    from ..engine import Engine

log = logging.getLogger("spread_watcher")

def _chunk_symbols(symbols: List[str], parts: int) -> List[List[str]]:
    parts = max(1, parts)
    if not symbols:
        return []
    parts = min(parts, len(symbols))
    chunks: List[List[str]] = [[] for _ in range(parts)]
    for i, sym in enumerate(symbols):
        chunks[i % parts].append(sym)
    return [c for c in chunks if c]


def _binance_shard_names(parts: int) -> List[str]:
    return [f"binance#{i + 1}" for i in range(max(1, parts))]


async def _binance_shard_client(engine: Engine, shard_idx: int, shard_total: int, shard_symbols: List[str]) -> None:
    streams = "/".join(f"{normalize_symbol(s).lower()}@bookTicker" for s in shard_symbols)
    url = f"{BINANCE_WS_BASE_URL}?streams={streams}"
    shard_name = f"binance#{shard_idx + 1}"

    async def sub(ws):
        return

    async def on_msg(ws, msg: Any):
        data = _loads(_to_text(msg))
        payload = data.get("data", data)
        sym = payload.get("s")
        if sym:
            engine.on_quote(
                "binance", sym,
                float(payload.get("b", 0.0)), float(payload.get("a", 0.0)),
                bid_sz=float(payload.get("B", 0.0) or 0.0),
                ask_sz=float(payload.get("A", 0.0) or 0.0),
                exch_ts_ns=int(payload.get("T") or payload.get("E") or 0) * 1_000_000,
                source_name=shard_name,
            )

    await _ws_loop(
        shard_name,
        url,
        sub,
        on_msg,
        stats=engine.conn_stats.get(shard_name),
        notify_fn=getattr(engine, "_notify", None),
        on_disconnect_fn=engine.invalidate_binance_shard,
    )


async def binance_client(engine: Engine) -> None:
    shard_count = max(1, min(BINANCE_WS_SHARDS, len(engine.symbols)))
    shards = _chunk_symbols(engine.symbols, shard_count)
    engine.set_binance_shards(shards)
    log.info("[binance] shards=%d symbols=%d distribution=%s", len(shards), len(engine.symbols), ",".join(str(len(s)) for s in shards))
    tasks = [
        asyncio.create_task(_binance_shard_client(engine, i, len(shards), shard_symbols))
        for i, shard_symbols in enumerate(shards)
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        engine.flush_resync_logs()
        for t in tasks:
            t.cancel()


