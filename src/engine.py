from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import TYPE_CHECKING, Deque, Dict, List, Optional, Tuple

from .config import (
    BASELINE_BUCKET_MS, BASELINE_WARMUP_MIN, BASELINE_WINDOW_MIN,
    BUFFER_MAX_AGE_NS, BUFFER_MAX_AGE_SEC, DISCOVERY_WARMUP_SEC,
    END_TH, ENTER_HOLD_MS, EX2I, EXCHANGES, EXIT_HOLD_MS, START_TH,
    TG_ENABLE,
)
from .models import EpisodeEvent, QuotePoint, SpreadEpisode
from .utils import normalize_symbol

if TYPE_CHECKING:
    from .monitoring import WsConnStats

log = logging.getLogger("spread_watcher")

class Engine:
    def __init__(self, symbols: List[str]):
        self.symbols = [normalize_symbol(s) for s in symbols]
        self.S = len(self.symbols)
        self.E = len(EXCHANGES)
        self.latest: List[List[Optional[QuotePoint]]] = [[None] * self.E for _ in range(self.S)]
        self.quote_buffers: List[List[Deque[QuotePoint]]] = [[deque() for _ in range(self.E)] for _ in range(self.S)]
        self.last_aligned_ts: Dict[Tuple[int, int, int], int] = {}
        self.states: Dict[Tuple[int, int, int], SpreadEpisode] = {}
        for si, sym in enumerate(self.symbols):
            for bi, b in enumerate(EXCHANGES):
                for sj, s in enumerate(EXCHANGES):
                    if bi == sj:
                        continue
                    self.states[(si, bi, sj)] = SpreadEpisode(
                        sym, b, s, START_TH, END_TH,
                        BASELINE_BUCKET_MS, BASELINE_WINDOW_MIN, BASELINE_WARMUP_MIN,
                        ENTER_HOLD_MS, EXIT_HOLD_MS,
                    )
        self.db_q: "asyncio.Queue[EpisodeEvent]" = asyncio.Queue(maxsize=20000)
        self.tg_q: "asyncio.Queue[str]" = asyncio.Queue(maxsize=20000)
        self.conn_stats: Dict[str, WsConnStats] = {}
        self.disabled: Dict[str, set[str]] = {ex: set() for ex in EXCHANGES}
        self.binance_symbol_shard: Dict[str, str] = {}
        self.discovery_done = False
        self.discovery_started_ns = time.perf_counter_ns()
        self.discovery_deadline_ns = self.discovery_started_ns + DISCOVERY_WARMUP_SEC * 1_000_000_000
        self.symbol_seen_exchanges: List[set[int]] = [set() for _ in range(self.S)]
        self.symbol_enabled: List[bool] = [False for _ in range(self.S)]
        self.pending_resync_symbols: Dict[str, str] = {}
        self._updates = 0
        self._last_stat = time.perf_counter()

    def bind(self) -> None:
        self._sym_map = {s: i for i, s in enumerate(self.symbols)}

    def note_symbol_exchange_seen(self, si: int, ex_idx: int) -> None:
        self.symbol_seen_exchanges[si].add(ex_idx)

    def finalize_discovery(self) -> None:
        if self.discovery_done:
            return
        enabled = 0
        disabled_examples: List[str] = []
        for si, seen in enumerate(self.symbol_seen_exchanges):
            self.symbol_enabled[si] = len(seen) >= 2
            if self.symbol_enabled[si]:
                enabled += 1
            elif len(disabled_examples) < 20:
                exch_names = ",".join(EXCHANGES[i] for i in sorted(seen)) or "-"
                disabled_examples.append(f"{self.symbols[si]}({exch_names})")
            self._clear_symbol(si)
        self.discovery_done = True
        log.info(
            "[discovery] enabled=%d/%d disabled=%d warmup=%ss%s",
            enabled,
            self.S,
            self.S - enabled,
            DISCOVERY_WARMUP_SEC,
            f" | examples: {', '.join(disabled_examples)}" if disabled_examples else "",
        )

    def queue_resync(self, si: int, reason: str) -> None:
        self.pending_resync_symbols[self.symbols[si]] = reason

    def flush_resync_logs(self) -> None:
        if not self.pending_resync_symbols:
            return
        items = sorted(self.pending_resync_symbols.items())
        preview = ", ".join(f"{sym}:{reason}" for sym, reason in items[:40])
        more = "" if len(items) <= 40 else f" ... +{len(items) - 40} more"
        log.warning("[resync] batch=%d | %s%s", len(items), preview, more)
        self.pending_resync_symbols.clear()

    def _pair_key(self, si: int, ea: int, eb: int) -> Tuple[int, int, int]:
        return (si, ea, eb) if ea < eb else (si, eb, ea)

    def _reset_symbol_pair_state(self, si: int, ex_idx: Optional[int] = None) -> None:
        if ex_idx is None:
            keys = [k for k in self.last_aligned_ts if k[0] == si]
        else:
            keys = [k for k in self.last_aligned_ts if k[0] == si and ex_idx in k[1:]]
        for key in keys:
            self.last_aligned_ts.pop(key, None)
        for ei in range(self.E):
            for oj in range(self.E):
                if ei == oj:
                    continue
                if ex_idx is not None and ei != ex_idx and oj != ex_idx:
                    continue
                self.states[(si, ei, oj)].abort_episode()

    def _clear_symbol(self, si: int) -> None:
        for ei in range(self.E):
            self.latest[si][ei] = None
            self.quote_buffers[si][ei].clear()
        self._reset_symbol_pair_state(si)

    def set_binance_shards(self, shards: List[List[str]]) -> None:
        self.binance_symbol_shard.clear()
        for idx, shard_symbols in enumerate(shards):
            shard_name = f"binance#{idx + 1}"
            for sym in shard_symbols:
                self.binance_symbol_shard[normalize_symbol(sym)] = shard_name

    def invalidate_exchange(self, ex: str) -> None:
        ei = EX2I[ex]
        for si in range(self.S):
            self.latest[si][ei] = None
            self.quote_buffers[si][ei].clear()
            self._reset_symbol_pair_state(si, ei)

    def invalidate_binance_shard(self, shard_name: str) -> None:
        ei = EX2I["binance"]
        for sym, mapped_shard in self.binance_symbol_shard.items():
            if mapped_shard != shard_name:
                continue
            si = self._sym_map.get(sym)
            if si is not None:
                self.latest[si][ei] = None
                self.quote_buffers[si][ei].clear()
                self._reset_symbol_pair_state(si, ei)

    def quote_valid_for_exchange(self, ex: str, qp: Optional[QuotePoint], sym: str) -> bool:
        if qp is None:
            return False
        if ex == "binance":
            expected_shard = self.binance_symbol_shard.get(sym)
            if expected_shard and qp.source_name != expected_shard:
                return False
            st = self.conn_stats.get(expected_shard or qp.source_name)
            return bool(st and st.connected)
        st = self.conn_stats.get(ex)
        return bool(st and st.connected)

    def _left_quote(self, buf: Deque[QuotePoint], t_ns: int) -> Optional[QuotePoint]:
        for qp in reversed(buf):
            if qp.exch_ts_ns <= t_ns:
                return qp
        return None

    def _snapshot_for_direction(
        self,
        si: int,
        buy_i: int,
        sell_i: int,
        t_ns: int,
    ) -> Optional[Tuple[int, float, float, float, float, float]]:
        """Return closest-left executable snapshot at exact t_ns.

        Tuple: (snapshot_ts_ns, spread, buy_ask, buy_ask_sz, sell_bid, sell_bid_sz).
        The timestamp returned is the exact requested hold boundary, while prices/sizes
        are from the freshest quotes with exch_ts <= that boundary.
        """
        buy_q = self._left_quote(self.quote_buffers[si][buy_i], t_ns)
        sell_q = self._left_quote(self.quote_buffers[si][sell_i], t_ns)
        if buy_q is None or sell_q is None:
            return None
        sym = self.symbols[si]
        buy_ex = EXCHANGES[buy_i]
        sell_ex = EXCHANGES[sell_i]
        if not self.quote_valid_for_exchange(buy_ex, buy_q, sym):
            return None
        if not self.quote_valid_for_exchange(sell_ex, sell_q, sym):
            return None
        if buy_q.ask <= 0:
            return None
        spread = max(0.0, (sell_q.bid - buy_q.ask) / buy_q.ask * 100.0)
        return (t_ns, spread, buy_q.ask, buy_q.ask_sz, sell_q.bid, sell_q.bid_sz)

    def _cleanup_pair_buffers(self, si: int, ea: int, eb: int, cutoff_ts: int) -> None:
        for ex_idx in (ea, eb):
            buf = self.quote_buffers[si][ex_idx]
            while len(buf) >= 2 and buf[1].exch_ts_ns <= cutoff_ts:
                buf.popleft()

    def _check_symbol_buffer_age(self, si: int) -> None:
        newest = 0
        oldest = None
        active_exchanges: List[str] = []
        for ei in range(self.E):
            buf = self.quote_buffers[si][ei]
            if not buf:
                continue
            active_exchanges.append(EXCHANGES[ei])
            newest = max(newest, buf[-1].exch_ts_ns)
            oldest = buf[0].exch_ts_ns if oldest is None else min(oldest, buf[0].exch_ts_ns)
        if oldest is None or newest <= 0:
            return
        if newest - oldest > BUFFER_MAX_AGE_NS:
            self.queue_resync(si, f"age>{BUFFER_MAX_AGE_SEC}s ex={','.join(active_exchanges)}")
            self._clear_symbol(si)

    def _process_aligned_pair(self, si: int, ea: int, eb: int) -> None:
        qa = self.quote_buffers[si][ea]
        qb = self.quote_buffers[si][eb]
        if len(qa) < 2 or len(qb) < 2:
            return
        watermark = min(qa[-1].exch_ts_ns, qb[-1].exch_ts_ns)
        if watermark <= 0:
            return
        key = self._pair_key(si, ea, eb)
        last_ts = self.last_aligned_ts.get(key, -1)
        candidate_times = sorted({q.exch_ts_ns for q in qa if last_ts < q.exch_ts_ns < watermark} | {q.exch_ts_ns for q in qb if last_ts < q.exch_ts_ns < watermark})
        if not candidate_times:
            return
        sym = self.symbols[si]
        latest_done = last_ts
        for t_ns in candidate_times:
            left_a = self._left_quote(qa, t_ns)
            left_b = self._left_quote(qb, t_ns)
            if left_a is None or left_b is None:
                continue
            ex_a = EXCHANGES[ea]
            ex_b = EXCHANGES[eb]
            if not self.quote_valid_for_exchange(ex_a, left_a, sym):
                continue
            if not self.quote_valid_for_exchange(ex_b, left_b, sym):
                continue
            spread_ab = max(0.0, (left_b.bid - left_a.ask) / left_a.ask * 100.0)
            state_ab = self.states[(si, ea, eb)]
            entry_snap_ab = None
            exit_snap_ab = None
            if (not state_ab.in_ep) and state_ab.enter_hold_ns > 0 and state_ab.enter_candidate_ts > 0 and t_ns - state_ab.enter_candidate_ts >= state_ab.enter_hold_ns:
                entry_snap_ab = self._snapshot_for_direction(si, ea, eb, state_ab.enter_candidate_ts + state_ab.enter_hold_ns)
            if state_ab.in_ep and state_ab.exit_hold_ns > 0 and state_ab.exit_candidate_ts > 0 and t_ns - state_ab.exit_candidate_ts >= state_ab.exit_hold_ns:
                exit_snap_ab = self._snapshot_for_direction(si, ea, eb, state_ab.exit_candidate_ts + state_ab.exit_hold_ns)
            ev_ab, _ = state_ab.on_tick(
                t_ns, spread_ab,
                buy_ask=left_a.ask, buy_ask_sz=left_a.ask_sz,
                sell_bid=left_b.bid, sell_bid_sz=left_b.bid_sz,
                entry_confirm_snapshot=entry_snap_ab,
                exit_confirm_snapshot=exit_snap_ab,
            )
            if ev_ab is not None:
                self._emit_episode(ev_ab)
            spread_ba = max(0.0, (left_a.bid - left_b.ask) / left_b.ask * 100.0)
            state_ba = self.states[(si, eb, ea)]
            entry_snap_ba = None
            exit_snap_ba = None
            if (not state_ba.in_ep) and state_ba.enter_hold_ns > 0 and state_ba.enter_candidate_ts > 0 and t_ns - state_ba.enter_candidate_ts >= state_ba.enter_hold_ns:
                entry_snap_ba = self._snapshot_for_direction(si, eb, ea, state_ba.enter_candidate_ts + state_ba.enter_hold_ns)
            if state_ba.in_ep and state_ba.exit_hold_ns > 0 and state_ba.exit_candidate_ts > 0 and t_ns - state_ba.exit_candidate_ts >= state_ba.exit_hold_ns:
                exit_snap_ba = self._snapshot_for_direction(si, eb, ea, state_ba.exit_candidate_ts + state_ba.exit_hold_ns)
            ev_ba, _ = state_ba.on_tick(
                t_ns, spread_ba,
                buy_ask=left_b.ask, buy_ask_sz=left_b.ask_sz,
                sell_bid=left_a.bid, sell_bid_sz=left_a.bid_sz,
                entry_confirm_snapshot=entry_snap_ba,
                exit_confirm_snapshot=exit_snap_ba,
            )
            if ev_ba is not None:
                self._emit_episode(ev_ba)
            latest_done = t_ns
        if latest_done > last_ts:
            self.last_aligned_ts[key] = latest_done
            self._cleanup_pair_buffers(si, ea, eb, latest_done)

    def on_quote(
        self,
        ex: str,
        sym: str,
        bid: float,
        ask: float,
        bid_sz: float = 0.0,
        ask_sz: float = 0.0,
        exch_ts_ns: Optional[int] = None,
        recv_ts_ns: Optional[int] = None,
        source_name: Optional[str] = None,
    ) -> None:
        try:
            if bid <= 0 or ask <= 0:
                return
            sym = normalize_symbol(sym)
            si = self._sym_map.get(sym)
            if si is None:
                return
            ei = EX2I[ex]
            self.note_symbol_exchange_seen(si, ei)
            recv_ns = recv_ts_ns if recv_ts_ns and recv_ts_ns > 0 else time.perf_counter_ns()
            ts_ns = exch_ts_ns if exch_ts_ns and exch_ts_ns > 0 else recv_ns
            qp = QuotePoint(ts_ns, recv_ns, bid, ask, max(0.0, bid_sz), max(0.0, ask_sz), source_name or ex)
            self.latest[si][ei] = qp
            if not self.discovery_done:
                return
            if not self.symbol_enabled[si]:
                return
            if not self.quote_valid_for_exchange(ex, qp, sym):
                return
            buf = self.quote_buffers[si][ei]
            if buf and ts_ns < buf[-1].exch_ts_ns:
                return
            if buf and ts_ns == buf[-1].exch_ts_ns:
                buf[-1] = qp
            else:
                buf.append(qp)
            self.latest[si][ei] = qp
            self._check_symbol_buffer_age(si)
            if self.latest[si][ei] is None:
                return
            for oj in range(self.E):
                if oj == ei:
                    continue
                if not self.symbol_enabled[si]:
                    break
                self._process_aligned_pair(si, ei, oj)
            self._updates += 1
            if self._updates % 50000 == 0:
                dt = time.perf_counter() - self._last_stat
                if dt > 0:
                    log.info("quotes processed: %d (%.0f / sec)", self._updates, self._updates / dt)
        except Exception:
            log.exception("on_quote error")

    def _emit_episode(self, ev: EpisodeEvent) -> None:
        try:
            self.db_q.put_nowait(ev)
        except asyncio.QueueFull:
            pass
        if TG_ENABLE:
            try:
                msg = (
                    f"{ev.symbol} {ev.buy_ex}→{ev.sell_ex} "
                    f"diff_start={ev.start_diff:.4f}% diff_max={ev.max_diff:.4f}% diff_end={ev.end_diff:.4f}% "
                    f"avg_start={ev.avg_start:.4f}% avg_end={ev.avg_end:.4f}% avg_roll={ev.avg_spread:.4f}% "
                    f"spread_start={ev.start_spread:.4f}% spread_max={ev.max_spread:.4f}% spread_end={ev.end_spread:.4f}% "
                    f"above={ev.dur_above_ms:.0f}ms total={ev.dur_total_ms:.0f}ms"
                )
                self.tg_q.put_nowait(msg)
            except asyncio.QueueFull:
                pass


