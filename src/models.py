from __future__ import annotations

import dataclasses
from collections import deque
from typing import Deque, Optional, Tuple

class TimeBucketBaseline:
    __slots__ = (
        "bucket_ms", "window_minutes", "max_points", "hist", "hist_sum",
        "cur_bucket_id", "cur_sum", "cur_count", "first_ts_ns",
        "last_bucket_value",
    )

    def __init__(self, bucket_ms: int, window_minutes: int):
        self.bucket_ms = max(1, bucket_ms)
        self.window_minutes = max(1, window_minutes)
        self.max_points = max(1, (self.window_minutes * 60_000) // self.bucket_ms)
        self.hist: Deque[float] = deque()
        self.hist_sum = 0.0
        self.cur_bucket_id = -1
        self.cur_sum = 0.0
        self.cur_count = 0
        self.first_ts_ns = 0
        # Last finalized 100ms bucket value. If no new quote appears in a bucket,
        # carry this value forward because the executable spread stayed unchanged.
        self.last_bucket_value: Optional[float] = None

    def has_data(self) -> bool:
        return bool(self.hist) or self.cur_count > 0

    def current(self) -> float:
        total_count = len(self.hist)
        total_sum = self.hist_sum
        if self.cur_count > 0:
            total_sum += self.cur_sum / self.cur_count
            total_count += 1
        if total_count > 0:
            return total_sum / total_count
        return 0.0

    def warmed_up(self, ts_ns: int, warmup_minutes: int) -> bool:
        if warmup_minutes <= 0:
            return self.has_data()
        if self.first_ts_ns <= 0 or ts_ns <= 0:
            return False
        return (ts_ns - self.first_ts_ns) >= warmup_minutes * 60 * 1_000_000_000

    def _push_finalized_value(self, avg: float) -> None:
        v = float(avg)
        self.last_bucket_value = v
        self.hist.append(v)
        self.hist_sum += v
        while len(self.hist) > self.max_points:
            self.hist_sum -= self.hist.popleft()

    def _finalize_current_bucket(self) -> None:
        if self.cur_bucket_id < 0 or self.cur_count <= 0:
            return
        self._push_finalized_value(self.cur_sum / self.cur_count)
        self.cur_sum = 0.0
        self.cur_count = 0

    def _fill_empty_buckets_with_last_value(self, target_bucket_id: int) -> None:
        if self.cur_bucket_id < 0 or self.last_bucket_value is None:
            return
        missing = int(target_bucket_id) - int(self.cur_bucket_id) - 1
        if missing <= 0:
            return
        # No need to push more than the whole rolling window: older repeated
        # values would immediately fall out of the deque anyway.
        for _ in range(min(missing, self.max_points)):
            self._push_finalized_value(float(self.last_bucket_value))

    def push(self, x: float, ts_ns: int) -> float:
        if self.first_ts_ns <= 0 and ts_ns > 0:
            self.first_ts_ns = ts_ns
        bucket_ns = self.bucket_ms * 1_000_000
        bucket_id = ts_ns // bucket_ns if ts_ns > 0 else 0
        if self.cur_bucket_id < 0:
            self.cur_bucket_id = bucket_id
        if bucket_id < self.cur_bucket_id:
            return self.current()
        if bucket_id == self.cur_bucket_id:
            self.cur_sum += x
            self.cur_count += 1
            return self.current()

        self._finalize_current_bucket()
        self._fill_empty_buckets_with_last_value(bucket_id)
        self.cur_bucket_id = bucket_id
        self.cur_sum = x
        self.cur_count = 1
        return self.current()


@dataclasses.dataclass(slots=True)
class QuotePoint:
    exch_ts_ns: int
    recv_ns: int
    bid: float
    ask: float
    bid_sz: float
    ask_sz: float
    source_name: str = ""


@dataclasses.dataclass(slots=True)
class EpisodeEvent:
    symbol: str
    buy_ex: str
    sell_ex: str
    # Legacy aliases kept for simple sorting/backward reading:
    # ts_start_ns = entry signal start, ts_end_ns = exit confirm time.
    ts_start_ns: int
    ts_end_ns: int
    entry_signal_ts_ns: int
    entry_confirm_ts_ns: int
    exit_signal_ts_ns: int
    exit_confirm_ts_ns: int
    start_spread: float
    max_spread: float
    end_spread: float
    start_diff: float
    max_diff: float
    end_diff: float
    avg_start: float
    avg_end: float
    dur_above_ms: float
    dur_total_ms: float
    start_buy_ask: float
    start_buy_ask_sz: float
    start_sell_bid: float
    start_sell_bid_sz: float
    end_buy_ask: float
    end_buy_ask_sz: float
    end_sell_bid: float
    end_sell_bid_sz: float
    avg_spread: float


class SpreadEpisode:
    __slots__ = (
        "symbol", "buy_ex", "sell_ex", "start_th", "end_th", "enter_hold_ns", "exit_hold_ns",
        "baseline", "warmup_min", "in_ep", "enter_candidate_ts", "exit_candidate_ts", "last_above_ts",
        "ts_start", "entry_confirm_ts", "exit_signal_ts", "avg_start", "avg_end",
        "start_spread", "start_diff", "max_spread", "max_diff",
        "start_buy_ask", "start_buy_ask_sz", "start_sell_bid", "start_sell_bid_sz",
        "pending_spreads",
    )

    def __init__(
        self,
        symbol: str,
        buy_ex: str,
        sell_ex: str,
        start_th: float,
        end_th: float,
        baseline_bucket_ms: int,
        baseline_window_min: int,
        warmup_min: int,
        enter_hold_ms: int,
        exit_hold_ms: int,
    ):
        self.symbol = symbol
        self.buy_ex = buy_ex
        self.sell_ex = sell_ex
        self.start_th = start_th
        self.end_th = end_th
        self.enter_hold_ns = max(0, enter_hold_ms) * 1_000_000
        self.exit_hold_ns = max(0, exit_hold_ms) * 1_000_000
        self.baseline = TimeBucketBaseline(baseline_bucket_ms, baseline_window_min)
        self.warmup_min = max(0, warmup_min)
        self.in_ep = False
        self.enter_candidate_ts = 0
        self.exit_candidate_ts = 0
        self.last_above_ts = 0
        self.ts_start = 0
        self.entry_confirm_ts = 0
        self.exit_signal_ts = 0
        self.avg_start = 0.0
        self.avg_end = 0.0
        self.start_spread = 0.0
        self.start_diff = 0.0
        self.max_spread = 0.0
        self.max_diff = 0.0
        self.start_buy_ask = 0.0
        self.start_buy_ask_sz = 0.0
        self.start_sell_bid = 0.0
        self.start_sell_bid_sz = 0.0
        self.pending_spreads: Deque[Tuple[int, float]] = deque()

    def _start_episode(
        self,
        signal_ts_ns: int,
        confirm_ts_ns: int,
        avg_prev: float,
        spread: float,
        diff: float,
        buy_ask: float,
        buy_ask_sz: float,
        sell_bid: float,
        sell_bid_sz: float,
    ) -> None:
        self.in_ep = True
        self.ts_start = signal_ts_ns
        self.entry_confirm_ts = confirm_ts_ns
        self.last_above_ts = confirm_ts_ns
        self.avg_start = avg_prev
        self.start_spread = spread
        self.start_diff = diff
        self.max_spread = spread
        self.max_diff = diff
        self.start_buy_ask = buy_ask
        self.start_buy_ask_sz = buy_ask_sz
        self.start_sell_bid = sell_bid
        self.start_sell_bid_sz = sell_bid_sz
        self.enter_candidate_ts = 0
        self.exit_candidate_ts = 0
        self.exit_signal_ts = 0
        self.pending_spreads.clear()
        self.pending_spreads.append((confirm_ts_ns, spread))

    def _flush_pending_into_baseline(self) -> float:
        avg_now = self.baseline.current()
        while self.pending_spreads:
            ts_ns, spread = self.pending_spreads.popleft()
            avg_now = self.baseline.push(spread, ts_ns)
        return avg_now

    def _reset(self) -> None:
        self.in_ep = False
        self.enter_candidate_ts = 0
        self.exit_candidate_ts = 0
        self.last_above_ts = 0
        self.ts_start = 0
        self.entry_confirm_ts = 0
        self.exit_signal_ts = 0
        self.avg_start = 0.0
        self.avg_end = 0.0
        self.start_spread = 0.0
        self.start_diff = 0.0
        self.max_spread = 0.0
        self.max_diff = 0.0
        self.start_buy_ask = 0.0
        self.start_buy_ask_sz = 0.0
        self.start_sell_bid = 0.0
        self.start_sell_bid_sz = 0.0
        self.pending_spreads.clear()

    def abort_episode(self) -> None:
        self._reset()

    def on_tick(
        self,
        ts_ns: int,
        spread: float,
        buy_ask: float,
        buy_ask_sz: float,
        sell_bid: float,
        sell_bid_sz: float,
        entry_confirm_snapshot: Optional[Tuple[int, float, float, float, float, float]] = None,
        exit_confirm_snapshot: Optional[Tuple[int, float, float, float, float, float]] = None,
    ) -> Tuple[Optional[EpisodeEvent], float]:
        avg_prev = self.baseline.current()
        if not self.baseline.has_data():
            avg_prev = spread

        if not self.in_ep:
            diff = spread - avg_prev

            # If the current tick arrived after the hold boundary, the current
            # spread may have already disappeared. For parser-vs-bot comparison we
            # must still evaluate the closest-left snapshot at the exact boundary.
            if self.enter_hold_ns > 0 and self.enter_candidate_ts > 0 and entry_confirm_snapshot is not None:
                confirm_ts, c_spread, c_buy_ask, c_buy_ask_sz, c_sell_bid, c_sell_bid_sz = entry_confirm_snapshot
                c_diff = c_spread - avg_prev
                if c_diff >= self.start_th and self.baseline.warmed_up(confirm_ts, self.warmup_min):
                    self._start_episode(self.enter_candidate_ts, confirm_ts, avg_prev, c_spread, c_diff, c_buy_ask, c_buy_ask_sz, c_sell_bid, c_sell_bid_sz)
                    return None, self.baseline.current()
                # The exact confirm snapshot failed, so this candidate is dead.
                self.enter_candidate_ts = 0

            if diff >= self.start_th and self.baseline.warmed_up(ts_ns, self.warmup_min):
                if self.enter_hold_ns == 0:
                    self._start_episode(ts_ns, ts_ns, avg_prev, spread, diff, buy_ask, buy_ask_sz, sell_bid, sell_bid_sz)
                    return None, self.baseline.current()
                if self.enter_candidate_ts == 0:
                    self.enter_candidate_ts = ts_ns
            else:
                self.enter_candidate_ts = 0
            avg_now = self.baseline.push(spread, ts_ns)
            return None, avg_now

        frozen_avg = self.avg_start
        diff = spread - frozen_avg

        should_end = False
        exit_confirm_ts = ts_ns
        end_spread = spread
        end_diff = diff
        end_buy_ask = buy_ask
        end_buy_ask_sz = buy_ask_sz
        end_sell_bid = sell_bid
        end_sell_bid_sz = sell_bid_sz

        # Same principle as entry: if a later tick reveals that the exit hold
        # boundary was already reached, use the closest-left snapshot at that exact
        # boundary even if the later/current tick has already recovered.
        if self.exit_hold_ns > 0 and self.exit_candidate_ts > 0 and exit_confirm_snapshot is not None:
            exit_confirm_ts, end_spread, end_buy_ask, end_buy_ask_sz, end_sell_bid, end_sell_bid_sz = exit_confirm_snapshot
            end_diff = end_spread - frozen_avg
            if end_diff <= self.end_th:
                should_end = True
                self.exit_signal_ts = self.exit_candidate_ts
            else:
                self.exit_candidate_ts = 0
                self.exit_signal_ts = 0

        if not should_end:
            if diff <= self.end_th:
                if self.exit_candidate_ts == 0:
                    self.exit_candidate_ts = ts_ns
                if self.exit_hold_ns == 0:
                    should_end = True
                    self.exit_signal_ts = ts_ns
                elif ts_ns - self.exit_candidate_ts >= self.exit_hold_ns:
                    should_end = True
                    self.exit_signal_ts = self.exit_candidate_ts
                    exit_confirm_ts = self.exit_candidate_ts + self.exit_hold_ns
                    end_diff = end_spread - frozen_avg
                    if end_diff > self.end_th:
                        should_end = False
                        self.exit_candidate_ts = 0
                        self.exit_signal_ts = 0
            else:
                self.exit_candidate_ts = 0
                self.exit_signal_ts = 0

        if not should_end:
            self.pending_spreads.append((ts_ns, spread))
            if diff >= self.start_th:
                self.last_above_ts = ts_ns
            if spread > self.max_spread:
                self.max_spread = spread
            if diff > self.max_diff:
                self.max_diff = diff
            return None, self.baseline.current()

        # End the episode at the exact exit confirm time, not at the later tick
        # that merely revealed that the hold window had elapsed.
        self.pending_spreads.append((exit_confirm_ts, end_spread))
        self.avg_end = frozen_avg
        dur_above_ms = (self.last_above_ts - self.ts_start) / 1_000_000 if self.last_above_ts and self.ts_start else 0.0
        dur_total_ms = (exit_confirm_ts - self.ts_start) / 1_000_000 if self.ts_start else 0.0
        avg_roll = self._flush_pending_into_baseline()
        ev = EpisodeEvent(
            symbol=self.symbol,
            buy_ex=self.buy_ex,
            sell_ex=self.sell_ex,
            ts_start_ns=self.ts_start,
            ts_end_ns=exit_confirm_ts,
            entry_signal_ts_ns=self.ts_start,
            entry_confirm_ts_ns=self.entry_confirm_ts or self.ts_start,
            exit_signal_ts_ns=self.exit_signal_ts or exit_confirm_ts,
            exit_confirm_ts_ns=exit_confirm_ts,
            start_spread=self.start_spread,
            max_spread=self.max_spread,
            end_spread=end_spread,
            start_diff=self.start_diff,
            max_diff=self.max_diff,
            end_diff=end_diff,
            avg_start=self.avg_start,
            avg_end=self.avg_end,
            dur_above_ms=dur_above_ms,
            dur_total_ms=dur_total_ms,
            start_buy_ask=self.start_buy_ask,
            start_buy_ask_sz=self.start_buy_ask_sz,
            start_sell_bid=self.start_sell_bid,
            start_sell_bid_sz=self.start_sell_bid_sz,
            end_buy_ask=end_buy_ask,
            end_buy_ask_sz=end_buy_ask_sz,
            end_sell_bid=end_sell_bid,
            end_sell_bid_sz=end_sell_bid_sz,
            avg_spread=avg_roll,
        )
        self._reset()
        return ev, avg_roll


