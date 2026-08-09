from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, List, Tuple

from .config import (
    BINANCE_WS_SHARDS, DISCOVERY_WARMUP_SEC, EX2I, EXCHANGES,
    RESOURCE_LOG_EVERY_SEC, RESYNC_LOG_EVERY_SEC, STATUS_LOG_EVERY_SEC,
)
from .exchanges.binance import _binance_shard_names

if TYPE_CHECKING:
    from .engine import Engine

log = logging.getLogger("spread_watcher")

class WsConnStats:
    __slots__ = (
        "name", "connected", "connects", "msgs_total", "last_msg_ns",
        "last_connect_ns", "last_disconnect_ns", "_last_report_t", "_last_report_msgs",
    )

    def __init__(self, name: str):
        self.name = name
        self.connected = False
        self.connects = 0
        self.msgs_total = 0
        self.last_msg_ns = 0
        self.last_connect_ns = 0
        self.last_disconnect_ns = 0
        self._last_report_t = time.perf_counter()
        self._last_report_msgs = 0

    def on_connect(self) -> None:
        self.connected = True
        self.connects += 1
        self.last_connect_ns = time.perf_counter_ns()

    def on_disconnect(self) -> None:
        self.connected = False
        self.last_disconnect_ns = time.perf_counter_ns()

    def on_msg(self) -> None:
        self.msgs_total += 1
        self.last_msg_ns = time.perf_counter_ns()

    def snapshot_rates(self) -> Tuple[float, int]:
        now_t = time.perf_counter()
        dt = now_t - self._last_report_t
        dmsg = self.msgs_total - self._last_report_msgs
        mpm = (dmsg / dt) * 60.0 if dt > 0 else 0.0
        self._last_report_t = now_t
        self._last_report_msgs = self.msgs_total
        age_ms = int((time.perf_counter_ns() - self.last_msg_ns) / 1_000_000) if self.last_msg_ns else 10**9
        return mpm, age_ms

async def discovery_warmup_task(engine: Engine, warmup_sec: int = DISCOVERY_WARMUP_SEC) -> None:
    await asyncio.sleep(max(1, warmup_sec))
    engine.finalize_discovery()


async def resync_log_flusher(engine: Engine, every_sec: int = RESYNC_LOG_EVERY_SEC) -> None:
    await asyncio.sleep(max(1, every_sec))
    while True:
        try:
            engine.flush_resync_logs()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("resync_log_flusher error")
        await asyncio.sleep(max(1, every_sec))


async def status_logger(engine: Engine, every_sec: int = STATUS_LOG_EVERY_SEC) -> None:
    await asyncio.sleep(max(1, every_sec))
    while True:
        try:
            lines: List[str] = []
            for ex in EXCHANGES:
                if ex == "binance":
                    shard_names = [name for name in _binance_shard_names(BINANCE_WS_SHARDS) if name in engine.conn_stats]
                    shard_stats = [engine.conn_stats[name] for name in shard_names]
                    if shard_stats:
                        shard_snaps = [(st, *st.snapshot_rates()) for st in shard_stats]
                        total_mpm = sum(mpm for _, mpm, _ in shard_snaps)
                        last_age = min(age for _, _, age in shard_snaps) if shard_snaps else 10**9
                        worst_age = max(age for _, _, age in shard_snaps) if shard_snaps else 10**9
                        connects = sum(st.connects for st, _, _ in shard_snaps)
                        connected = sum(1 for st, _, _ in shard_snaps if st.connected)
                        ei = EX2I[ex]
                        present = 0
                        stale: List[Tuple[int, str]] = []
                        now_recv = time.perf_counter_ns()
                        for si, sym in enumerate(engine.symbols):
                            qp = engine.latest[si][ei]
                            if qp is None:
                                continue
                            present += 1
                            age_ms = int((now_recv - qp.recv_ns) / 1_000_000)
                            stale.append((age_ms, f"{sym}:{age_ms}ms"))
                        stale.sort(reverse=True)
                        dist = ",".join(
                            f"{name}={'up' if st.connected else 'down'}:{int(mpm)}"
                            for st, mpm, _age in shard_snaps
                            for name in [st.name]
                        )
                        lines.append(
                            f"{ex:<6}: connected={connected}/{len(shard_stats)} connects={connects} msgs/min={int(total_mpm)} "
                            f"last_msg_age={last_age}ms worst_shard_age={worst_age}ms | states={present}/{len(engine.symbols)} | shards: {dist} | stalest: {', '.join(x for _, x in stale[:3]) if stale else '-'}"
                        )
                        continue
                st = engine.conn_stats.get(ex)
                if st is None:
                    continue
                mpm, last_age = st.snapshot_rates()
                ei = EX2I[ex]
                present = 0
                stale: List[Tuple[int, str]] = []
                now_recv = time.perf_counter_ns()
                for si, sym in enumerate(engine.symbols):
                    qp = engine.latest[si][ei]
                    if qp is None:
                        continue
                    present += 1
                    age_ms = int((now_recv - qp.recv_ns) / 1_000_000)
                    stale.append((age_ms, f"{sym}:{age_ms}ms"))
                stale.sort(reverse=True)
                lines.append(
                    f"{ex:<6}: connected={st.connected} connects={st.connects} msgs/min={int(mpm)} "
                    f"last_msg_age={last_age}ms | states={present}/{len(engine.symbols)} | stalest: {', '.join(x for _, x in stale[:3]) if stale else '-'}"
                )
            if lines:
                log.info("\n" + "\n".join(lines))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("status_logger error")
        await asyncio.sleep(max(1, every_sec))


def _read_proc_net_bytes() -> Tuple[int, int]:
    rx = 0
    tx = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if ":" not in line:
                    continue
                iface, rest = line.split(":", 1)
                if iface.strip() == "lo":
                    continue
                parts = rest.split()
                if len(parts) >= 16:
                    rx += int(parts[0])
                    tx += int(parts[8])
    except Exception:
        pass
    return rx, tx


async def resource_logger(every_sec: int = RESOURCE_LOG_EVERY_SEC) -> None:
    try:
        import resource
    except Exception:
        resource = None  # type: ignore
    last_t = time.perf_counter()
    last_rx, last_tx = _read_proc_net_bytes()
    await asyncio.sleep(max(1, every_sec))
    while True:
        try:
            now = time.perf_counter()
            rx, tx = _read_proc_net_bytes()
            dt = max(0.001, now - last_t)
            rx_mbps = ((rx - last_rx) * 8.0 / dt) / 1_000_000
            tx_mbps = ((tx - last_tx) * 8.0 / dt) / 1_000_000
            rss_mb = 0.0
            if resource is not None:
                rss_mb = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024.0
            log.info("[resource] rss_max=%.1fMB net_rx=%.3fMbps net_tx=%.3fMbps", rss_mb, rx_mbps, tx_mbps)
            last_t, last_rx, last_tx = now, rx, tx
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("resource_logger error")
        await asyncio.sleep(max(1, every_sec))


async def boot_report(engine: Engine, delay_sec: int = 5) -> None:
    await asyncio.sleep(max(1, delay_sec))
    try:
        for ex in EXCHANGES:
            ei = EX2I[ex]
            have = sum(1 for si in range(engine.S) if engine.latest[si][ei] is not None)
            log.info("[BOOT] %s states=%d/%d", ex, have, engine.S)
    except Exception:
        log.exception("boot_report error")


