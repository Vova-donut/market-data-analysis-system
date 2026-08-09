#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import logging

from .config import (
    BASELINE_BUCKET_MS, BASELINE_WINDOW_MIN, BINANCE_WS_BASE_URL,
    BINANCE_WS_SHARDS, BUFFER_MAX_AGE_SEC, DB_PATH, DISCOVERY_WARMUP_SEC,
    END_TH, ENTER_HOLD_MS, EXCHANGES, EXIT_HOLD_MS, OKX_USE_SBE,
    RESOURCE_LOG_EVERY_SEC, RESYNC_LOG_EVERY_SEC, START_TH,
    STATUS_LOG_EVERY_SEC, SYMBOLS, TG_ENABLE, WS_MAX_QUEUE,
)
from .engine import Engine
from .exchanges.binance import _binance_shard_names, binance_client
from .exchanges.bitget import bitget_client
from .exchanges.bybit import bybit_client
from .exchanges.okx import okx_client, okx_sbe_client
from .monitoring import (
    WsConnStats, boot_report, discovery_warmup_task, resource_logger,
    resync_log_flusher, status_logger,
)
from .storage import db_writer
from .telegram_notifications import tg_prepare, tg_sender
from .utils import normalize_symbol

log = logging.getLogger("spread_watcher")


async def main() -> None:
    symbols = [normalize_symbol(s) for s in SYMBOLS]
    if not symbols:
        raise SystemExit("SYMBOLS is empty")
    engine = Engine(symbols)
    engine.bind()
    engine.conn_stats = {ex: WsConnStats(ex) for ex in EXCHANGES if ex != "binance"}
    if "binance" in EXCHANGES:
        for name in _binance_shard_names(max(1, min(BINANCE_WS_SHARDS, len(symbols)))):
            engine.conn_stats[name] = WsConnStats(name)

    def _notify(text: str) -> None:
        if TG_ENABLE:
            try:
                engine.tg_q.put_nowait(text)
            except asyncio.QueueFull:
                pass
        else:
            log.warning("%s", text)

    engine._notify = _notify  # type: ignore[attr-defined]
    log.info(
        "symbols=%d exchanges=%d active=%s | START_TH=%.4f%% END_TH=%.4f%% BASELINE_BUCKET_MS=%d BASELINE_WINDOW_MIN=%d ENTER_HOLD_MS=%d EXIT_HOLD_MS=%d BINANCE_WS_SHARDS=%d BINANCE_WS_BASE_URL=%s STATUS_LOG_EVERY_SEC=%d BUFFER_MAX_AGE_SEC=%d DISCOVERY_WARMUP_SEC=%d RESYNC_LOG_EVERY_SEC=%d WS_MAX_QUEUE=%d",
        len(symbols), len(EXCHANGES), ",".join(EXCHANGES), START_TH, END_TH,
        BASELINE_BUCKET_MS, BASELINE_WINDOW_MIN, ENTER_HOLD_MS, EXIT_HOLD_MS, BINANCE_WS_SHARDS, BINANCE_WS_BASE_URL, STATUS_LOG_EVERY_SEC, BUFFER_MAX_AGE_SEC, DISCOVERY_WARMUP_SEC, RESYNC_LOG_EVERY_SEC, WS_MAX_QUEUE,
    )
    tg_client_chat = await tg_prepare() if TG_ENABLE else None
    tasks = [
        asyncio.create_task(db_writer(DB_PATH, engine.db_q)),
        asyncio.create_task(boot_report(engine, delay_sec=5)),
        asyncio.create_task(discovery_warmup_task(engine, warmup_sec=DISCOVERY_WARMUP_SEC)),
        asyncio.create_task(resync_log_flusher(engine, every_sec=RESYNC_LOG_EVERY_SEC)),
        asyncio.create_task(status_logger(engine, every_sec=STATUS_LOG_EVERY_SEC)),
        asyncio.create_task(resource_logger(every_sec=RESOURCE_LOG_EVERY_SEC)),
    ]
    if "binance" in EXCHANGES:
        tasks.append(asyncio.create_task(binance_client(engine)))
    if "bybit" in EXCHANGES:
        tasks.append(asyncio.create_task(bybit_client(engine)))
    if "okx" in EXCHANGES:
        tasks.append(asyncio.create_task(okx_sbe_client(engine) if OKX_USE_SBE else okx_client(engine)))
    if "bitget" in EXCHANGES:
        tasks.append(asyncio.create_task(bitget_client(engine)))
    if TG_ENABLE:
        tasks.insert(1, asyncio.create_task(tg_sender(engine.tg_q, tg_client_chat)))
    engine._notify(
        f"🟢 Parser started | symbols={len(symbols)} exchanges={len(EXCHANGES)} active={','.join(EXCHANGES)} | "
        f"START_TH={START_TH:.4f}% END_TH={END_TH:.4f}% BASELINE_BUCKET_MS={BASELINE_BUCKET_MS} "
        f"BASELINE_WINDOW_MIN={BASELINE_WINDOW_MIN} ENTER_HOLD_MS={ENTER_HOLD_MS} EXIT_HOLD_MS={EXIT_HOLD_MS} "
        f"BINANCE_WS_SHARDS={BINANCE_WS_SHARDS} BINANCE_WS_BASE_URL={BINANCE_WS_BASE_URL} STATUS_LOG_EVERY_SEC={STATUS_LOG_EVERY_SEC} BUFFER_MAX_AGE_SEC={BUFFER_MAX_AGE_SEC} DISCOVERY_WARMUP_SEC={DISCOVERY_WARMUP_SEC} RESYNC_LOG_EVERY_SEC={RESYNC_LOG_EVERY_SEC} WS_MAX_QUEUE={WS_MAX_QUEUE}"
    )
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        engine.flush_resync_logs()
        for t in tasks:
            t.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
