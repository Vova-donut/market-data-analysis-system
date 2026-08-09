from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, List

from ..utils import normalize_symbol
from ..websocket import _dumps, _loads, _to_text, _ws_loop

if TYPE_CHECKING:
    from ..engine import Engine

log = logging.getLogger("spread_watcher")

async def bybit_client(engine: Engine) -> None:
    url = "wss://stream.bybit.com/v5/public/linear"

    def _topics() -> List[str]:
        active = [s for s in engine.symbols if s not in engine.disabled.get("bybit", set())]
        return [f"orderbook.1.{normalize_symbol(s)}" for s in active]

    async def sub(ws):
        topics = _topics()
        if topics:
            await ws.send(_dumps({"op": "subscribe", "args": topics}))

    async def on_msg(ws, msg: Any):
        data = _loads(_to_text(msg))
        if not isinstance(data, dict):
            return
        if data.get("op") == "subscribe" and data.get("success") is False:
            ret = str(data.get("ret_msg") or "")
            log.warning("[bybit] subscribe failed ret_msg=%s", ret)
            if "topic:" in ret:
                bad_topic = ret.split("topic:", 1)[-1].strip()
                bad_sym = normalize_symbol(bad_topic.split(".")[-1].strip())
                if bad_sym:
                    engine.disabled.setdefault("bybit", set()).add(bad_sym)
                    try:
                        await sub(ws)
                    except Exception:
                        pass
            return
        topic = data.get("topic")
        if not isinstance(topic, str) or not topic.startswith("orderbook."):
            return
        d = data.get("data")
        if not isinstance(d, dict):
            return
        bids = d.get("b") or []
        asks = d.get("a") or []
        if not bids or not asks:
            return
        sym = d.get("s") or topic.split(".")[-1]
        try:
            bid = float(bids[0][0]); ask = float(asks[0][0])
            bid_sz = float(bids[0][1]) if len(bids[0]) > 1 else 0.0
            ask_sz = float(asks[0][1]) if len(asks[0]) > 1 else 0.0
        except Exception:
            return
        engine.on_quote("bybit", sym, bid, ask, bid_sz, ask_sz, int(d.get("cts") or d.get("ts") or data.get("ts") or 0) * 1_000_000, source_name="bybit")

    await _ws_loop("bybit", url, sub, on_msg, stats=engine.conn_stats.get("bybit"), notify_fn=getattr(engine, "_notify", None), ping_interval=20.0, on_disconnect_fn=lambda _name: engine.invalidate_exchange("bybit"))


