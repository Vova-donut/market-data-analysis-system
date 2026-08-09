from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..utils import normalize_symbol
from ..websocket import _dumps, _loads, _to_text, _ws_loop

if TYPE_CHECKING:
    from ..engine import Engine

async def bitget_client(engine: Engine) -> None:
    url = "wss://ws.bitget.com/v2/ws/public"
    args = [{"instType": "USDT-FUTURES", "channel": "books1", "instId": normalize_symbol(s)} for s in engine.symbols]

    async def sub(ws):
        await ws.send(_dumps({"op": "subscribe", "args": args}))

    async def on_msg(ws, msg: Any):
        text = _to_text(msg)
        if text == "pong":
            return
        data = _loads(text)
        if not isinstance(data, dict) or data.get("event") or data.get("action") not in ("snapshot", "update"):
            return
        arg = data.get("arg") or {}
        sym = arg.get("instId")
        lst = data.get("data")
        if not sym or not isinstance(lst, list) or not lst:
            return
        ob = lst[0]
        bids = ob.get("bids")
        asks = ob.get("asks")
        if not bids or not asks:
            return
        bid = float(bids[0][0]); ask = float(asks[0][0])
        bid_sz = float(bids[0][1]) if len(bids[0]) > 1 else 0.0
        ask_sz = float(asks[0][1]) if len(asks[0]) > 1 else 0.0
        engine.on_quote("bitget", sym, bid, ask, bid_sz, ask_sz, int(ob.get("ts") or data.get("ts") or 0) * 1_000_000, source_name="bitget")

    await _ws_loop("bitget", url, sub, on_msg, stats=engine.conn_stats.get("bitget"), notify_fn=getattr(engine, "_notify", None), ping_text="ping", ping_every=30.0, ping_interval=None, on_disconnect_fn=lambda _name: engine.invalidate_exchange("bitget"))


