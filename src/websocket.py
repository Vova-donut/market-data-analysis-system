from __future__ import annotations

import asyncio
import inspect
import json
import logging
import random
import time
from typing import Any, Dict, Optional

import websockets

from .config import WS_CLOSE_TIMEOUT_SEC, WS_MAX_QUEUE, WS_PING_TIMEOUT_SEC

log = logging.getLogger("spread_watcher")

def _to_text(msg: Any) -> str:
    if isinstance(msg, str):
        return msg
    if isinstance(msg, (bytes, bytearray)):
        b = bytes(msg)
        try:
            s = b.decode("utf-8", "ignore")
            if s and (s[0] == "{" or s[0] == "["):
                return s
        except Exception:
            s = ""
        try:
            import gzip
            s2 = gzip.decompress(b).decode("utf-8", "ignore")
            if s2:
                return s2
        except Exception:
            pass
        try:
            import zlib
            return zlib.decompress(b).decode("utf-8", "ignore")
        except Exception:
            pass
        try:
            import zlib
            return zlib.decompress(b, -zlib.MAX_WBITS).decode("utf-8", "ignore")
        except Exception:
            pass
        return s or ""
    return ""


try:
    import orjson  # type: ignore

    def _loads(b: bytes | str) -> Any:
        return orjson.loads(b)

    def _dumps(o: Any) -> str:
        return orjson.dumps(o).decode("utf-8")
except Exception:
    def _loads(b: bytes | str) -> Any:
        if isinstance(b, (bytes, bytearray)):
            b = b.decode("utf-8", "ignore")
        return json.loads(b)

    def _dumps(o: Any) -> str:
        return json.dumps(o, separators=(",", ":"), ensure_ascii=False)

async def _ws_loop(
    name: str,
    url: str,
    subscribe_fn,
    message_fn,
    stats: Optional[WsConnStats] = None,
    notify_fn: Optional[Any] = None,
    ping_text: Optional[str] = None,
    ping_every: float = 30.0,
    ping_interval: Optional[float] = 20.0,
    on_connect_fn = None,
    on_disconnect_fn = None,
    extra_headers: Optional[Dict[str, str]] = None,
) -> None:
    try:
        wants_ws = len(inspect.signature(message_fn).parameters) >= 2
    except Exception:
        wants_ws = False
    backoff = 0.25
    connected_started_ns = 0
    while True:
        try:
            connect_kwargs = dict(
                ping_interval=ping_interval,
                ping_timeout=WS_PING_TIMEOUT_SEC,
                compression=None,
                max_queue=WS_MAX_QUEUE,
                close_timeout=WS_CLOSE_TIMEOUT_SEC,
            )
            if extra_headers:
                try:
                    headers_now = extra_headers() if callable(extra_headers) else extra_headers
                    params = inspect.signature(websockets.connect).parameters
                    if "additional_headers" in params:
                        connect_kwargs["additional_headers"] = headers_now
                    elif "extra_headers" in params:
                        connect_kwargs["extra_headers"] = headers_now
                    else:
                        connect_kwargs["user_agent_header"] = headers_now.get("User-Agent") if isinstance(headers_now, dict) else None
                except Exception:
                    headers_now = extra_headers() if callable(extra_headers) else extra_headers
                    connect_kwargs["extra_headers"] = headers_now
            async with websockets.connect(url, **connect_kwargs) as ws:
                connected_started_ns = time.perf_counter_ns()
                if stats is not None:
                    prev = stats.connected
                    stats.on_connect()
                    if on_connect_fn is not None:
                        on_connect_fn(name)
                    if not prev:
                        log.info("[%s] connected", name)
                        if notify_fn is not None:
                            notify_fn(f"✅ {name} WS connected")
                await subscribe_fn(ws)
                ping_task = None
                if ping_text is not None and ping_every and ping_every > 0:
                    async def _pinger() -> None:
                        while True:
                            await asyncio.sleep(ping_every)
                            try:
                                await ws.send(ping_text)
                            except Exception:
                                return
                    ping_task = asyncio.create_task(_pinger())
                backoff = 0.25
                async for msg in ws:
                    if stats is not None:
                        stats.on_msg()
                    if ping_text is not None and msg in ("Ping", "ping", b"Ping", b"ping"):
                        try:
                            await ws.send(ping_text)
                        except Exception:
                            pass
                        continue
                    if wants_ws:
                        await message_fn(ws, msg)
                    else:
                        await message_fn(msg)
                if ping_task:
                    ping_task.cancel()
                if stats is not None:
                    prev = stats.connected
                    stats.on_disconnect()
                    if on_disconnect_fn is not None:
                        on_disconnect_fn(name)
                    if prev:
                        uptime_ms = int((time.perf_counter_ns() - connected_started_ns) / 1_000_000) if connected_started_ns else -1
                        log.warning("[%s] disconnected cleanly uptime=%dms", name, uptime_ms)
                        if notify_fn is not None:
                            notify_fn(f"⚠️ {name} WS disconnected")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            close_code = getattr(e, "code", None)
            close_reason = getattr(e, "reason", None)
            uptime_ms = int((time.perf_counter_ns() - connected_started_ns) / 1_000_000) if connected_started_ns else -1
            log.warning("[%s] ws error: %s | code=%s reason=%s uptime=%dms", name, repr(e), close_code, close_reason, uptime_ms)
            if stats is not None and stats.connected:
                stats.on_disconnect()
                if on_disconnect_fn is not None:
                    on_disconnect_fn(name)
                if notify_fn is not None:
                    notify_fn(f"⚠️ {name} WS error: {type(e).__name__}")
            await asyncio.sleep(backoff + random.random() * 0.25)
            backoff = min(5.0, backoff * 1.6)


