#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import dataclasses
import inspect
import json
import logging
import os
import base64
import hashlib
import hmac
import struct
import xml.etree.ElementTree as ET
import random
import re
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import websockets
from dotenv import load_dotenv


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


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


MANUAL_SYMBOLS: List[str] = ['0GUSDT', '1INCHUSDT', '2ZUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ACTUSDT', 'ACUUSDT', 'ADAUSDT', 'AEROUSDT', 'AEVOUSDT', 'AGLDUSDT', 'AIXBTUSDT', 'ALGOUSDT', 'ALLOUSDT', 'ANIMEUSDT', 'APEUSDT', 'API3USDT', 'APRUSDT',
                             'APTUSDT', 'ARBUSDT', 'ARKMUSDT', 'ARUSDT', 'ASTERUSDT', 'ATHUSDT', 'ATOMUSDT', 'ATUSDT', 'AUCTIONUSDT', 'AUSDT', 'AVAXUSDT', 'AVNTUSDT', 'AXSUSDT', 'AZTECUSDT', 'BABYUSDT', 'BANDUSDT', 'BARDUSDT', 'BATUSDT', 'BCHUSDT',
                             'BEATUSDT', 'BERAUSDT', 'BICOUSDT', 'BIGTIMEUSDT', 'BIOUSDT', 'BLURUSDT', 'BNBUSDT', 'BOMEUSDT', 'BRETTUSDT', 'BREVUSDT', 'CCUSDT', 'CELOUSDT', 'CFXUSDT', 'CHZUSDT', 'COAIUSDT', 'COMPUSDT', 'CRVUSDT', 'CVXUSDT', 'DASHUSDT',
                             'DOGEUSDT', 'DOODUSDT', 'DOTUSDT', 'DYDXUSDT', 'EDENUSDT', 'EGLDUSDT', 'EIGENUSDT', 'ENAUSDT', 'ENJUSDT', 'ENSOUSDT', 'ENSUSDT', 'ESPUSDT', 'ETCUSDT', 'ETHFIUSDT', 'ETHWUSDT', 'FARTCOINUSDT', 'FILUSDT', 'FLOWUSDT',
                             'FOGOUSDT', 'FUNUSDT', 'FUSDT', 'GALAUSDT', 'GASUSDT', 'GIGGLEUSDT', 'GLMUSDT', 'GMTUSDT', 'GMXUSDT', 'GPSUSDT', 'GRASSUSDT', 'GRTUSDT', 'HBARUSDT', 'HMSTRUSDT', 'HOMEUSDT', 'HUMAUSDT', 'HUSDT', 'HYPEUSDT', 'ICPUSDT',
                             'ICXUSDT', 'IMXUSDT', 'INITUSDT', 'INJUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IPUSDT', 'JELLYJELLYUSDT', 'JTOUSDT', 'JUPUSDT', 'KAITOUSDT', 'KGENUSDT', 'KMNOUSDT', 'KSMUSDT', 'LABUSDT', 'LAUSDT', 'LAYERUSDT', 'LDOUSDT',
                             'LIGHTUSDT', 'LINEAUSDT', 'LINKUSDT', 'LITUSDT', 'LPTUSDT', 'LQTYUSDT', 'LRCUSDT', 'LTCUSDT', 'MAGICUSDT', 'MANAUSDT', 'MASKUSDT', 'MEMEUSDT', 'MERLUSDT', 'METISUSDT', 'METUSDT', 'MEUSDT', 'MEWUSDT', 'MINAUSDT', 'MMTUSDT',
                             'MONUSDT', 'MOODENGUSDT', 'MORPHOUSDT', 'MOVEUSDT', 'MUBARAKUSDT', 'NEARUSDT', 'NEIROUSDT', 'NEOUSDT', 'NIGHTUSDT', 'NMRUSDT', 'NOTUSDT', 'OLUSDT', 'OMUSDT', 'ONDOUSDT', 'ONEUSDT', 'ONTUSDT', 'OPUSDT', 'ORDERUSDT',
                             'ORDIUSDT', 'PARTIUSDT', 'PENDLEUSDT', 'PENGUUSDT', 'PEOPLEUSDT', 'PIEVERSEUSDT', 'PIPPINUSDT', 'PLUMEUSDT', 'PNUTUSDT', 'POLUSDT', 'POPCATUSDT', 'PROMPTUSDT', 'PROVEUSDT', 'PUMPUSDT', 'PYTHUSDT', 'QTUMUSDT',
                             'RAVEUSDT', 'RECALLUSDT', 'RENDERUSDT', 'RESOLVUSDT', 'RIVERUSDT', 'RLSUSDT', 'RSRUSDT', 'RVNUSDT', 'SAHARAUSDT', 'SANDUSDT', 'SAPIENUSDT', 'SEIUSDT', 'SENTUSDT', 'SHELLUSDT', 'SIGNUSDT', 'SKYUSDT', 'SNXUSDT',
                             'SOONUSDT', 'SOPHUSDT', 'SPACEUSDT', 'SPKUSDT', 'SPXUSDT', 'SSVUSDT', 'STABLEUSDT', 'STRKUSDT', 'STXUSDT', 'SUIUSDT', 'SUSDT', 'SUSHIUSDT', 'SYRUPUSDT', 'TAOUSDT', 'THETAUSDT', 'TIAUSDT', 'TONUSDT', 'TRBUSDT',
                             'TRIAUSDT', 'TRUMPUSDT', 'TRUSTUSDT', 'TRUTHUSDT', 'TRXUSDT', 'TURBOUSDT', 'UMAUSDT', 'UNIUSDT', 'USDCUSDT', 'USELESSUSDT', 'VANAUSDT', 'VIRTUALUSDT', 'WALUSDT', 'WCTUSDT', 'WETUSDT', 'WIFUSDT', 'WLDUSDT', 'WLFIUSDT',
                             'WOOUSDT', 'WUSDT', 'XANUSDT', 'XLMUSDT', 'XPLUSDT', 'XRPUSDT', 'XTZUSDT', 'YBUSDT', 'YFIUSDT', 'YGGUSDT', 'ZAMAUSDT', 'ZBTUSDT', 'ZECUSDT', 'ZENUSDT', 'ZETAUSDT', 'ZILUSDT', 'ZKPUSDT', 'ZKUSDT', 'ZORAUSDT', 'ZROUSDT',
                             'ZRXUSDT'
                            ]

SYMBOLS_RAW = os.getenv("SYMBOLS", "").strip()
SYMBOLS = [s.strip() for s in SYMBOLS_RAW.split(",") if s.strip()] if SYMBOLS_RAW else MANUAL_SYMBOLS


def _norm_sym_init(s: str) -> str:
    s = (s or "").strip().upper()
    for ch in ["/", "-", ":", " "]:
        s = s.replace(ch, "")
    return s


_seen_syms: set[str] = set()
_normed: list[str] = []
for _s in SYMBOLS:
    _ns = _norm_sym_init(_s)
    if _ns and _ns not in _seen_syms:
        _seen_syms.add(_ns)
        _normed.append(_ns)
SYMBOLS = _normed

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ENV_PATH)
load_dotenv()

START_TH = _env_float("START_TH", 0.3)
END_TH = _env_float("END_TH", 0.001)
BASELINE_BUCKET_MS = _env_int("BASELINE_BUCKET_MS", 100)
BASELINE_WINDOW_MIN = _env_int("BASELINE_WINDOW_MIN", 60)
BASELINE_WARMUP_MIN = _env_int("BASELINE_WARMUP_MIN", 60)
ENTER_HOLD_MS = _env_int("ENTER_HOLD_MS", 0)
EXIT_HOLD_MS = _env_int("EXIT_HOLD_MS", 0)
DB_PATH = os.getenv("DB_PATH", "spreads.db")
TG_ENABLE = os.getenv("TG_ENABLE", "0").strip() == "1"
TG_API_ID = (os.getenv("API_ID") or "").strip()
TG_API_HASH = (os.getenv("API_HASH") or "").strip()
TG_PHONE = (os.getenv("PHONE_NUMBER") or "").strip()
TG_PASSWORD_2FA = (os.getenv("TG_PASSWORD") or "").strip()
TG_CHAT_ID = (os.getenv("TARGET_CHAT_VLAD") or "").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()
BINANCE_WS_SHARDS = max(1, _env_int("BINANCE_WS_SHARDS", 6))
STATUS_LOG_EVERY_SEC = max(10, _env_int("STATUS_LOG_EVERY_SEC", 180))
BUFFER_MAX_AGE_SEC = max(5, _env_int("BUFFER_MAX_AGE_SEC", 60))
BUFFER_MAX_AGE_NS = BUFFER_MAX_AGE_SEC * 1_000_000_000
DISCOVERY_WARMUP_SEC = max(10, _env_int("DISCOVERY_WARMUP_SEC", 120))
RESYNC_LOG_EVERY_SEC = max(30, _env_int("RESYNC_LOG_EVERY_SEC", 180))
WS_MAX_QUEUE = max(256, _env_int("WS_MAX_QUEUE", 2048))
WS_PING_TIMEOUT_SEC = max(5.0, _env_float("WS_PING_TIMEOUT_SEC", 20.0))
WS_CLOSE_TIMEOUT_SEC = max(1.0, _env_float("WS_CLOSE_TIMEOUT_SEC", 2.0))
BINANCE_WS_BASE_URL = os.getenv("BINANCE_WS_BASE_URL", "wss://fstream.binance.com/public/stream").strip().rstrip("?")
RESOURCE_LOG_EVERY_SEC = max(10, _env_int("RESOURCE_LOG_EVERY_SEC", 180))

OKX_USE_SBE = os.getenv("OKX_USE_SBE", "1").strip() == "1"
OKX_API_KEY_PARSER = (os.getenv("OKX_API_KEY_PARSER") or "").strip()
OKX_SECRET_PARSER = (os.getenv("OKX_SECRET_PARSER") or "").strip()
OKX_PASSWORD_PARSER = (os.getenv("OKX_PASSWORD_PARSER") or "").strip()
OKX_SBE_WS_URL = os.getenv("OKX_SBE_WS_URL", "wss://ws.okx.com:8443/ws/v5/public-sbe").strip()
OKX_SBE_WS_URLS = [x.strip() for x in os.getenv("OKX_SBE_WS_URLS", f"{OKX_SBE_WS_URL},wss://wsaws.okx.com:8443/ws/v5/public-sbe").split(",") if x.strip()]
OKX_SBE_SCHEMA_PATH = os.getenv("OKX_SBE_SCHEMA_PATH", "okx-sbe-schema.xml").strip()
OKX_SBE_SCHEMA_URL = os.getenv("OKX_SBE_SCHEMA_URL", "https://www.okx.com/docs-v5/log_en/xml/okx_sbe_1_0.xml").strip()
OKX_SBE_LOG_UNKNOWN_EVERY = max(1, _env_int("OKX_SBE_LOG_UNKNOWN_EVERY", 50000))
OKX_SBE_SAMPLE_LOG_EVERY_SEC = max(0, _env_int("OKX_SBE_SAMPLE_LOG_EVERY_SEC", 0))


ALL_EXCHANGES = ["binance", "okx"]
ACTIVE_EXCHANGES_RAW = os.getenv("ACTIVE_EXCHANGES", "binance,okx").strip()
_active_seen: set[str] = set()
ACTIVE_EXCHANGES: List[str] = []
for _name in [x.strip().lower() for x in ACTIVE_EXCHANGES_RAW.split(",") if x.strip()]:
    if _name in ALL_EXCHANGES and _name not in _active_seen:
        _active_seen.add(_name)
        ACTIVE_EXCHANGES.append(_name)
if len(ACTIVE_EXCHANGES) < 2:
    raise SystemExit("ACTIVE_EXCHANGES must contain at least 2 supported exchanges")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
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

def _ns_to_utc_text(ts_ns: int) -> str:
    try:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).isoformat(timespec="milliseconds")
    except Exception:
        return ""


def _db_init(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_dt TEXT NOT NULL DEFAULT '',
            entry_signal_dt TEXT NOT NULL DEFAULT '',
            entry_confirm_dt TEXT NOT NULL DEFAULT '',
            exit_signal_dt TEXT NOT NULL DEFAULT '',
            exit_confirm_dt TEXT NOT NULL DEFAULT '',
            ts_start_ns INTEGER NOT NULL,
            ts_end_ns   INTEGER NOT NULL,
            entry_signal_ts_ns INTEGER NOT NULL DEFAULT 0,
            entry_confirm_ts_ns INTEGER NOT NULL DEFAULT 0,
            exit_signal_ts_ns INTEGER NOT NULL DEFAULT 0,
            exit_confirm_ts_ns INTEGER NOT NULL DEFAULT 0,
            symbol      TEXT NOT NULL,
            buy_ex      TEXT NOT NULL,
            sell_ex     TEXT NOT NULL,
            start_spread REAL NOT NULL,
            max_spread   REAL NOT NULL,
            end_spread   REAL NOT NULL DEFAULT 0,
            start_diff   REAL NOT NULL DEFAULT 0,
            max_diff     REAL NOT NULL DEFAULT 0,
            end_diff     REAL NOT NULL DEFAULT 0,
            avg_start    REAL NOT NULL DEFAULT 0,
            avg_end      REAL NOT NULL DEFAULT 0,
            dur_above_ms REAL NOT NULL DEFAULT 0,
            dur_total_ms REAL NOT NULL DEFAULT 0,
            start_buy_ask REAL NOT NULL,
            start_buy_ask_sz REAL NOT NULL DEFAULT 0,
            start_buy_ask_usd REAL NOT NULL DEFAULT 0,
            start_sell_bid REAL NOT NULL,
            start_sell_bid_sz REAL NOT NULL DEFAULT 0,
            start_sell_bid_usd REAL NOT NULL DEFAULT 0,
            end_buy_ask REAL NOT NULL,
            end_buy_ask_sz REAL NOT NULL DEFAULT 0,
            end_buy_ask_usd REAL NOT NULL DEFAULT 0,
            end_sell_bid REAL NOT NULL,
            end_sell_bid_sz REAL NOT NULL DEFAULT 0,
            end_sell_bid_usd REAL NOT NULL DEFAULT 0,
            avg_spread REAL NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_episodes_symbol_time ON episodes(symbol, ts_end_ns);")

    cols = {
        "start_dt": "TEXT NOT NULL DEFAULT ''",
        "entry_signal_dt": "TEXT NOT NULL DEFAULT ''",
        "entry_confirm_dt": "TEXT NOT NULL DEFAULT ''",
        "exit_signal_dt": "TEXT NOT NULL DEFAULT ''",
        "exit_confirm_dt": "TEXT NOT NULL DEFAULT ''",
        "entry_signal_ts_ns": "INTEGER NOT NULL DEFAULT 0",
        "entry_confirm_ts_ns": "INTEGER NOT NULL DEFAULT 0",
        "exit_signal_ts_ns": "INTEGER NOT NULL DEFAULT 0",
        "exit_confirm_ts_ns": "INTEGER NOT NULL DEFAULT 0",
        "end_spread": "REAL NOT NULL DEFAULT 0",
        "start_diff": "REAL NOT NULL DEFAULT 0",
        "max_diff": "REAL NOT NULL DEFAULT 0",
        "end_diff": "REAL NOT NULL DEFAULT 0",
        "avg_start": "REAL NOT NULL DEFAULT 0",
        "avg_end": "REAL NOT NULL DEFAULT 0",
        "dur_above_ms": "REAL NOT NULL DEFAULT 0",
        "dur_total_ms": "REAL NOT NULL DEFAULT 0",
        "start_buy_ask_sz": "REAL NOT NULL DEFAULT 0",
        "start_buy_ask_usd": "REAL NOT NULL DEFAULT 0",
        "start_sell_bid_sz": "REAL NOT NULL DEFAULT 0",
        "start_sell_bid_usd": "REAL NOT NULL DEFAULT 0",
        "end_buy_ask_sz": "REAL NOT NULL DEFAULT 0",
        "end_buy_ask_usd": "REAL NOT NULL DEFAULT 0",
        "end_sell_bid_sz": "REAL NOT NULL DEFAULT 0",
        "end_sell_bid_usd": "REAL NOT NULL DEFAULT 0",
    }
    cur.execute("PRAGMA table_info(episodes);")
    existing = {row[1] for row in cur.fetchall()}
    for name, ddl in cols.items():
        if name not in existing:
            cur.execute(f"ALTER TABLE episodes ADD COLUMN {name} {ddl};")
    conn.commit()


async def db_writer(db_path: str, q: "asyncio.Queue[EpisodeEvent]") -> None:
    conn = sqlite3.connect(db_path, isolation_level=None, check_same_thread=False)
    _db_init(conn)
    cur = conn.cursor()
    batch: List[EpisodeEvent] = []
    last_flush = time.perf_counter()

    while True:
        try:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=0.25)
                batch.append(ev)
            except asyncio.TimeoutError:
                pass
            now = time.perf_counter()
            if batch and (len(batch) >= 200 or (now - last_flush) >= 0.25):
                cur.executemany(
                    """
                    INSERT INTO episodes (
                        start_dt, entry_signal_dt, entry_confirm_dt, exit_signal_dt, exit_confirm_dt,
                        ts_start_ns, ts_end_ns, entry_signal_ts_ns, entry_confirm_ts_ns, exit_signal_ts_ns, exit_confirm_ts_ns,
                        symbol, buy_ex, sell_ex,
                        start_spread, max_spread, end_spread,
                        start_diff, max_diff, end_diff,
                        avg_start, avg_end,
                        dur_above_ms, dur_total_ms,
                        start_buy_ask, start_buy_ask_sz, start_buy_ask_usd,
                        start_sell_bid, start_sell_bid_sz, start_sell_bid_usd,
                        end_buy_ask, end_buy_ask_sz, end_buy_ask_usd,
                        end_sell_bid, end_sell_bid_sz, end_sell_bid_usd,
                        avg_spread
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            _ns_to_utc_text(e.ts_start_ns),
                            _ns_to_utc_text(e.entry_signal_ts_ns),
                            _ns_to_utc_text(e.entry_confirm_ts_ns),
                            _ns_to_utc_text(e.exit_signal_ts_ns),
                            _ns_to_utc_text(e.exit_confirm_ts_ns),
                            e.ts_start_ns,
                            e.ts_end_ns,
                            e.entry_signal_ts_ns,
                            e.entry_confirm_ts_ns,
                            e.exit_signal_ts_ns,
                            e.exit_confirm_ts_ns,
                            e.symbol,
                            e.buy_ex,
                            e.sell_ex,
                            e.start_spread,
                            e.max_spread,
                            e.end_spread,
                            e.start_diff,
                            e.max_diff,
                            e.end_diff,
                            e.avg_start,
                            e.avg_end,
                            e.dur_above_ms,
                            e.dur_total_ms,
                            e.start_buy_ask,
                            e.start_buy_ask_sz,
                            e.start_buy_ask * e.start_buy_ask_sz,
                            e.start_sell_bid,
                            e.start_sell_bid_sz,
                            e.start_sell_bid * e.start_sell_bid_sz,
                            e.end_buy_ask,
                            e.end_buy_ask_sz,
                            e.end_buy_ask * e.end_buy_ask_sz,
                            e.end_sell_bid,
                            e.end_sell_bid_sz,
                            e.end_sell_bid * e.end_sell_bid_sz,
                            e.avg_spread,
                        )
                        for e in batch
                    ],
                )
                batch.clear()
                last_flush = now
        except asyncio.CancelledError:
            break
        except Exception:
            log.exception("DB writer error")
            await asyncio.sleep(0.2)


async def tg_prepare() -> Optional[Tuple[Any, Any]]:
    if not TG_ENABLE:
        return None
    if not (TG_API_ID and TG_API_HASH and TG_PHONE and TG_CHAT_ID):
        log.warning("TG_ENABLE=1 but telegram env is incomplete.")
        return None
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.errors import SessionPasswordNeededError  # type: ignore
    except Exception:
        log.warning("TG_ENABLE=1 but Telethon is not installed.")
        return None
    session_name = os.getenv("TG_SESSION_NAME", "parser")
    client = TelegramClient(session_name, int(TG_API_ID), TG_API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(TG_PHONE)
        try:
            code = await asyncio.to_thread(input, "Telegram code (from app/SMS): ")
        except Exception:
            await client.disconnect()
            return None
        try:
            await client.sign_in(TG_PHONE, code)
        except SessionPasswordNeededError:
            pw = TG_PASSWORD_2FA or await asyncio.to_thread(input, "Telegram 2FA password: ")
            await client.sign_in(password=pw)
    try:
        chat_ref: str | int = TG_CHAT_ID.strip()
        if re.fullmatch(r"-?\d+", str(chat_ref)):
            chat_ref = int(str(chat_ref))
        try:
            await client.get_dialogs(limit=200)
        except Exception:
            pass
        chat_entity = await client.get_input_entity(chat_ref)
    except Exception:
        log.exception("Failed to resolve TARGET_CHAT_VLAD=%s", TG_CHAT_ID)
        await client.disconnect()
        return None
    return client, chat_entity


async def tg_sender(q: "asyncio.Queue[str]", client_chat: Optional[Tuple[Any, Any]]) -> None:
    if not client_chat:
        while True:
            _ = await q.get()
    client, chat_entity = client_chat
    try:
        while True:
            try:
                text = await q.get()
                await client.send_message(chat_entity, text)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("TG send error")
                await asyncio.sleep(0.5)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


EXCHANGES = ACTIVE_EXCHANGES
EX2I = {n: i for i, n in enumerate(EXCHANGES)}


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



class GenericSbeDecoder:
    """
    Schema-driven SBE decoder for OKX market data.
    It uses the official OKX XML schema at runtime. No float guessing.
    """
    PRIMITIVES = {
        "char": ("c", 1),
        "int8": ("b", 1), "uint8": ("B", 1),
        "int16": ("h", 2), "uint16": ("H", 2),
        "int32": ("i", 4), "uint32": ("I", 4),
        "int64": ("q", 8), "uint64": ("Q", 8),
        "float": ("f", 4), "float32": ("f", 4),
        "double": ("d", 8), "float64": ("d", 8),
    }

    def __init__(self, schema_path: str):
        self.schema_path = schema_path
        self.types: Dict[str, Dict[str, Any]] = {}
        self.messages: Dict[int, Dict[str, Any]] = {}
        self.header_size = 8
        self.header_encoding = {"blockLength": "uint16", "templateId": "uint16", "schemaId": "uint16", "version": "uint16"}
        self._load_schema(schema_path)

    @staticmethod
    def _strip_ns(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    def _children(self, node: ET.Element, name: Optional[str] = None) -> List[ET.Element]:
        return [ch for ch in list(node) if name is None or self._strip_ns(ch.tag) == name]

    def _load_schema(self, schema_path: str) -> None:
        path = Path(schema_path).expanduser()
        if not path.exists():
            try:
                import urllib.request
                log.warning("[OKX SBE] schema not found at %s, downloading from %s", schema_path, OKX_SBE_SCHEMA_URL)
                req = urllib.request.Request(OKX_SBE_SCHEMA_URL, headers={"User-Agent": "Mozilla/5.0"})
                data = urllib.request.urlopen(req, timeout=20).read()
                path.write_bytes(data)
                log.info("[OKX SBE] downloaded schema bytes=%d to %s", len(data), path)
            except Exception as e:
                raise FileNotFoundError(
                    f"OKX_SBE_SCHEMA_PATH not found: {schema_path}. Download official schema from {OKX_SBE_SCHEMA_URL} "
                    f"and put it there, or set OKX_SBE_SCHEMA_PATH. Download error: {e!r}"
                )
        root = ET.parse(path).getroot()
        for types_node in self._children(root, "types"):
            for node in list(types_node):
                tag = self._strip_ns(node.tag)
                name = node.attrib.get("name")
                if not name:
                    continue
                self.types[name] = {"kind": tag, "attrib": dict(node.attrib), "node": node}
                if tag == "composite" and name in ("messageHeader", "MessageHeader"):
                    encs: Dict[str, str] = {}
                    for t in self._children(node, "type"):
                        n = t.attrib.get("name")
                        enc = t.attrib.get("primitiveType") or t.attrib.get("type") or t.attrib.get("encodingType")
                        if n and enc:
                            encs[n] = enc
                    if encs:
                        self.header_encoding = encs
                        self.header_size = sum(self._primitive_size(v) for v in encs.values()) or 8

        for msg in self._children(root, "message"):
            tid = msg.attrib.get("id") or msg.attrib.get("templateId")
            if not tid:
                continue
            try:
                template_id = int(tid)
            except Exception:
                continue
            self.messages[template_id] = {
                "name": msg.attrib.get("name", f"template_{template_id}"),
                "blockLength": int(msg.attrib.get("blockLength", "0") or 0),
                "node": msg,
            }
        log.info("[OKX SBE] loaded schema=%s messages=%d types=%d header_size=%d", schema_path, len(self.messages), len(self.types), self.header_size)

    def _primitive_size(self, primitive: str) -> int:
        primitive = (primitive or "").strip()
        if primitive in self.PRIMITIVES:
            return self.PRIMITIVES[primitive][1]
        t = self.types.get(primitive)
        if not t:
            return 0
        if t["kind"] == "type":
            if t["attrib"].get("presence") == "constant":
                return 0
            length = int(t["attrib"].get("length", "1") or 1)
            enc = t["attrib"].get("primitiveType") or t["attrib"].get("type") or t["attrib"].get("encodingType") or "uint8"
            return self._primitive_size(enc) * length
        if t["kind"] in ("enum", "set"):
            enc = t["attrib"].get("encodingType") or t["attrib"].get("primitiveType") or "uint8"
            return self._primitive_size(enc)
        if t["kind"] == "composite":
            max_end = 0
            cur = 0
            for ch in self._children(t["node"]):
                rel = int(ch.attrib.get("offset", str(cur)) or cur)
                sz = self._primitive_size(ch.attrib.get("type") or ch.attrib.get("primitiveType") or ch.attrib.get("encodingType") or "")
                max_end = max(max_end, rel + sz)
                cur = rel + sz
            return max_end
        return int(t["attrib"].get("encodedLength", "0") or 0)

    def _read_primitive(self, b: bytes, off: int, primitive: str, length: int = 1) -> Tuple[Any, int]:
        primitive = (primitive or "").strip()
        if primitive in self.PRIMITIVES:
            fmt, size = self.PRIMITIVES[primitive]
            if off + size * length > len(b):
                raise ValueError("buffer too small")
            if primitive == "char":
                raw = b[off:off + length]
                return raw.rstrip(b"\x00").decode("ascii", "ignore"), off + length
            if length == 1:
                return struct.unpack_from("<" + fmt, b, off)[0], off + size
            return list(struct.unpack_from("<" + fmt * length, b, off)), off + size * length

        t = self.types.get(primitive)
        if not t:
            raise ValueError(f"unknown type {primitive}")
        if t["kind"] == "type":
            if t["attrib"].get("presence") == "constant":
                val = t["attrib"].get("value") or (t["node"].text or "")
                return val, off
            length = int(t["attrib"].get("length", "1") or 1)
            enc = t["attrib"].get("primitiveType") or t["attrib"].get("type") or t["attrib"].get("encodingType") or "uint8"
            return self._read_primitive(b, off, enc, length)
        if t["kind"] in ("enum", "set"):
            enc = t["attrib"].get("encodingType") or t["attrib"].get("primitiveType") or "uint8"
            return self._read_primitive(b, off, enc, 1)
        if t["kind"] == "composite":
            d: Dict[str, Any] = {}
            cur = off
            max_end = off
            for ch in self._children(t["node"]):
                name = ch.attrib.get("name") or self._strip_ns(ch.tag)
                typ = ch.attrib.get("type") or ch.attrib.get("primitiveType") or ch.attrib.get("encodingType") or ""
                rel = int(ch.attrib.get("offset", str(cur - off)) or (cur - off))
                val, end = self._read_primitive(b, off + rel, typ)
                d[name] = val
                cur = end
                max_end = max(max_end, end)
            return d, max_end
        raise ValueError(f"unsupported type kind {t['kind']}")

    def decode_header(self, b: bytes) -> Dict[str, int]:
        if len(b) < 8:
            raise ValueError("SBE message too short")
        out: Dict[str, int] = {}
        off = 0
        for n, enc in self.header_encoding.items():
            v, off = self._read_primitive(b, off, enc)
            if isinstance(v, int):
                out[n] = v
        if "templateId" not in out:
            bl, tid, sid, ver = struct.unpack_from("<HHHH", b, 0)
            out.update({"blockLength": bl, "templateId": tid, "schemaId": sid, "version": ver})
        return out

    def _decode_fixed_fields(self, msg_node: ET.Element, b: bytes, base: int, block_len: int) -> Tuple[Dict[str, Any], int]:
        d: Dict[str, Any] = {}
        max_end = base
        for f in self._children(msg_node, "field"):
            name = f.attrib.get("name") or "field"
            typ = f.attrib.get("type") or f.attrib.get("primitiveType") or f.attrib.get("encodingType") or ""
            offset = int(f.attrib.get("offset", "0") or 0)
            try:
                val, end = self._read_primitive(b, base + offset, typ)
                d[name] = val
                max_end = max(max_end, end)
            except Exception as e:
                d[name] = None
                log.debug("[OKX SBE] failed field %s type=%s: %s", name, typ, e)
        return d, max(base + block_len, max_end)

    def _decode_group(self, group_node: ET.Element, b: bytes, off: int) -> Tuple[str, List[Dict[str, Any]], int]:
        name = group_node.attrib.get("name") or "group"
        if off + 4 > len(b):
            return name, [], off
        block_len, num = struct.unpack_from("<HH", b, off)
        off += 4
        rows: List[Dict[str, Any]] = []
        for _ in range(num):
            row: Dict[str, Any] = {}
            row_base = off
            max_end = row_base + block_len
            for f in self._children(group_node, "field"):
                fname = f.attrib.get("name") or "field"
                typ = f.attrib.get("type") or f.attrib.get("primitiveType") or f.attrib.get("encodingType") or ""
                rel = int(f.attrib.get("offset", "0") or 0)
                try:
                    row[fname], end = self._read_primitive(b, row_base + rel, typ)
                    max_end = max(max_end, end)
                except Exception:
                    row[fname] = None
            off = max_end
            for ng in self._children(group_node, "group"):
                gname, gvals, off = self._decode_group(ng, b, off)
                row[gname] = gvals
            rows.append(row)
        return name, rows, off

    def decode(self, msg: bytes) -> Dict[str, Any]:
        header = self.decode_header(msg)
        tid = int(header.get("templateId", -1))
        m = self.messages.get(tid)
        if not m:
            return {"_header": header, "_unknown_template": tid, "_raw_len": len(msg)}
        base = self.header_size
        block_len = int(header.get("blockLength") or m.get("blockLength") or 0)
        data, off = self._decode_fixed_fields(m["node"], msg, base, block_len)
        for g in self._children(m["node"], "group"):
            gname, gvals, off = self._decode_group(g, msg, off)
            data[gname] = gvals
        data["_header"] = header
        data["_template"] = m["name"]
        return data


def _walk_values(obj: Any):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from _walk_values(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk_values(x)


def _as_float(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x)
        except Exception:
            return None
    if isinstance(x, dict):
        keys = {str(k).lower(): v for k, v in x.items()}
        mant = keys.get("mantissa", keys.get("num", keys.get("value")))
        exp = keys.get("exponent", keys.get("scale", 0))
        if mant is not None:
            try:
                return float(mant) * (10.0 ** int(exp))
            except Exception:
                return None
    return None


def _find_numeric_by_names(decoded: Dict[str, Any], needles: Tuple[str, ...]) -> Optional[float]:
    for k, v in _walk_values(decoded):
        kl = str(k).lower()
        if any(n in kl for n in needles):
            fv = _as_float(v)
            if fv is not None and fv > 0:
                return fv
    return None


def _extract_okx_sbe_quote(decoded: Dict[str, Any], inst_map: Dict[int, str]) -> Optional[Tuple[str, float, float, float, float, int]]:
    inst_code_val: Optional[int] = None
    for k, v in _walk_values(decoded):
        kl = str(k).lower()
        if kl in ("instidcode", "instrumentid", "instrumentidcode"):
            if isinstance(v, int):
                inst_code_val = v
                break
            if isinstance(v, str) and v.isdigit():
                inst_code_val = int(v)
                break

    sym = inst_map.get(inst_code_val or -1, "")
    if not sym:
        for k, v in _walk_values(decoded):
            if str(k).lower() == "instid" and isinstance(v, str) and v:
                sym = normalize_symbol(v.replace("SWAP", ""))
                break
    if not sym:
        return None

    bid = _find_numeric_by_names(decoded, ("bidpx", "bidprice", "bid_px", "bestbid"))
    ask = _find_numeric_by_names(decoded, ("askpx", "askprice", "ask_px", "bestask"))
    bid_sz = _find_numeric_by_names(decoded, ("bidsz", "bidsize", "bidqty", "bid_qty")) or 0.0
    ask_sz = _find_numeric_by_names(decoded, ("asksz", "asksize", "askqty", "ask_qty")) or 0.0
    ts_val = _find_numeric_by_names(decoded, ("timestamp", "updatetime", "ts")) or 0.0

    if bid is None or ask is None or bid <= 0 or ask <= 0:
        return None
    if bid > 1_000_000 and ask > 1_000_000:
        for scale in (1e-8, 1e-9, 1e-6):
            sb, sa = bid * scale, ask * scale
            if 0 < sb < 1_000_000 and 0 < sa < 1_000_000:
                bid, ask = sb, sa
                break

    ts_ns = int(ts_val) * 1_000_000 if 1_000_000_000_000 <= ts_val < 10_000_000_000_000 else int(ts_val)
    return sym, bid, ask, bid_sz, ask_sz, ts_ns


def _decimal_scale_from_str(value: Any, default: float = 100.0) -> float:
    """
    Convert an OKX decimal step string into an integer scale.

    Examples:
      "0.001"   -> 1000.0
      "0.00001" -> 100000.0
      "0.1"     -> 10.0
      "1"       -> 1.0

    OKX SBE BBO prices are sent as integer ticks, so using a fixed /100
    is wrong for symbols with different tickSz.
    """
    try:
        text = str(value or "").strip().lower()
        if not text:
            return float(default)
        if "e-" in text:
            return float(10 ** int(text.split("e-", 1)[1]))
        if "e+" in text:
            return float(10 ** -int(text.split("e+", 1)[1]))
        if "." not in text:
            return 1.0
        frac = text.split(".", 1)[1].rstrip("0")
        return float(10 ** len(frac)) if frac else 1.0
    except Exception:
        return float(default)


async def _okx_fetch_inst_code_map(symbols: List[str]) -> Dict[int, Dict[str, Any]]:
    import urllib.request
    out: Dict[int, Dict[str, Any]] = {}
    try:
        urls = [
            os.getenv("OKX_REST_BASE_URL", "https://www.okx.com").rstrip("/") + "/api/v5/public/instruments?instType=SWAP",
            "https://aws.okx.com/api/v5/public/instruments?instType=SWAP",
        ]
        data = None
        last_err = None
        for url in urls:
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) parser/05",
                    "Accept": "application/json",
                    "Cache-Control": "no-cache",
                })
                with urllib.request.urlopen(req, timeout=10) as r:
                    data = json.loads(r.read().decode("utf-8"))
                log.info("[OKX SBE] fetched instruments from %s", url)
                break
            except Exception as e:
                last_err = e
                log.warning("[OKX SBE] instruments fetch failed url=%s err=%r", url, e)
        if data is None:
            raise RuntimeError(f"all instrument endpoints failed; last={last_err!r}")
        wanted = {okx_inst_id(s): normalize_symbol(s) for s in symbols}
        for row in data.get("data", []):
            inst = row.get("instId")
            code = row.get("instIdCode")
            if inst in wanted and code is not None:
                try:
                    tick_sz = row.get("tickSz") or row.get("tickSize") or "0.01"
                    lot_sz = row.get("lotSz") or row.get("minSz") or "0.01"
                    out[int(code)] = {
                        "symbol": wanted[inst],
                        # Price scale must follow tickSz. Fixed /100 produced 10x/100x
                        # errors for symbols such as RIVER, APIS, BIO, etc.
                        "price_scale": _decimal_scale_from_str(tick_sz, 100.0),
                        # Size scale is less critical for spread calculation; use lotSz
                        # dynamically, with /100 fallback matching the previous decoder.
                        "size_scale": _decimal_scale_from_str(lot_sz, 100.0),
                        "tickSz": str(tick_sz),
                        "lotSz": str(lot_sz),
                    }
                except Exception:
                    pass
    except Exception:
        log.exception("[OKX SBE] failed to fetch instIdCode map")
    log.info("[OKX SBE] instIdCode mapped=%d/%d", len(out), len(symbols))
    return out



def _decode_okx_sbe_bbo_tbt(msg: bytes, inst_code_map: Dict[int, Dict[str, Any]]) -> Optional[Tuple[str, float, float, float, float, int, int]]:
    """
    Fast hardcoded decoder for OKX SBE public-sbe bbo-tbt.

    Confirmed against OKX JSON bbo-tbt on 2026-04-30:
    - WebSocket payload len: 82 bytes
    - SBE header: blockLength=74, templateId=1000, schemaId=1, version=0
    - price scale: raw / price_scale from OKX tickSz per instrument
    - size scale: raw / size_scale from OKX lotSz per instrument
    - timestamp: microseconds, convert to ns for Engine
    """
    if len(msg) < 82:
        return None
    try:
        block_len, template_id, schema_id, version = struct.unpack_from("<HHHH", msg, 0)
        if block_len != 74 or template_id != 1000:
            return None

        inst_code = struct.unpack_from("<Q", msg, 8)[0]
        inst_info = inst_code_map.get(int(inst_code))
        if not inst_info:
            return None
        sym = str(inst_info.get("symbol") or "")
        if not sym:
            return None
        price_scale = float(inst_info.get("price_scale") or 100.0)
        size_scale = float(inst_info.get("size_scale") or 100.0)
        if price_scale <= 0:
            price_scale = 100.0
        if size_scale <= 0:
            size_scale = 100.0

        ts_us = struct.unpack_from("<Q", msg, 16)[0]
        seq = struct.unpack_from("<Q", msg, 32)[0]

        ask_px_raw = struct.unpack_from("<q", msg, 40)[0]
        ask_sz_raw = struct.unpack_from("<q", msg, 48)[0]
        bid_px_raw = struct.unpack_from("<q", msg, 56)[0]
        bid_sz_raw = struct.unpack_from("<q", msg, 64)[0]

        bid = bid_px_raw / price_scale
        ask = ask_px_raw / price_scale
        bid_sz = bid_sz_raw / size_scale
        ask_sz = ask_sz_raw / size_scale
        ts_ns = int(ts_us) * 1000

        if bid <= 0 or ask <= 0:
            return None
        return sym, bid, ask, bid_sz, ask_sz, ts_ns, int(seq)
    except Exception:
        return None


async def okx_sbe_client(engine: Engine) -> None:
    if not (OKX_API_KEY_PARSER and OKX_SECRET_PARSER and OKX_PASSWORD_PARSER):
        raise SystemExit("OKX_USE_SBE=1 but OKX_API_KEY_PARSER / OKX_SECRET_PARSER / OKX_PASSWORD_PARSER are missing")

    inst_code_map = await _okx_fetch_inst_code_map(engine.symbols)
    mapped_symbols = {str(v.get("symbol") or "") for v in inst_code_map.values()}
    missing = [s for s in engine.symbols if normalize_symbol(s) not in mapped_symbols]
    if missing:
        log.warning(
            "[OKX SBE] skipping %d symbols without instIdCode map, examples=%s",
            len(missing),
            ",".join(missing[:20]),
        )
    if not inst_code_map:
        raise SystemExit("[OKX SBE] no instIdCode values mapped; cannot subscribe to SBE")

    try:
        sample_scales = ", ".join(
            f"{v.get('symbol')}:tick={v.get('tickSz')} scale={int(float(v.get('price_scale') or 0))}"
            for _, v in list(sorted(inst_code_map.items()))[:8]
        )
        log.info("[OKX SBE] price scales loaded examples=%s", sample_scales)
    except Exception:
        pass

    args = [{"channel": "bbo-tbt", "instIdCode": code} for code in sorted(inst_code_map.keys())]
    unknown_count = 0
    decoded_count = 0
    last_decode_log = time.perf_counter()

    def _sign(ts: str) -> str:
        # OKX SBE public-sbe requires OK-ACCESS-* headers during the HTTP WebSocket handshake.
        # This is the same signing payload as OKX WebSocket login verification.
        msg = f"{ts}GET/users/self/verify"
        return base64.b64encode(
            hmac.new(OKX_SECRET_PARSER.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

    def _auth_headers() -> Dict[str, str]:
        # Must be generated fresh on each reconnect. A stale timestamp can cause 401/60012.
        ts = str(time.time())
        return {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) parser/06",
            "Origin": "https://www.okx.com",
            "OK-ACCESS-KEY": OKX_API_KEY_PARSER,
            "OK-ACCESS-SIGN": _sign(ts),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": OKX_PASSWORD_PARSER,
            "x-simulated-trading": "0",
        }

    async def sub(ws):
        # No separate login message for public-sbe. Auth is done in handshake headers.
        log.info("[OKX SBE] subscribing bbo-tbt instruments=%d", len(args))
        for i in range(0, len(args), 50):
            await ws.send(_dumps({"op": "subscribe", "args": args[i:i + 50]}))
            await asyncio.sleep(0.05)

    async def on_msg(ws, msg: Any):
        nonlocal unknown_count, decoded_count, last_decode_log

        if isinstance(msg, str) or (isinstance(msg, (bytes, bytearray)) and bytes(msg[:1]) in (b"{", b"[")):
            try:
                data = _loads(_to_text(msg))
            except Exception:
                return
            ev = data.get("event") if isinstance(data, dict) else None
            if ev == "subscribe":
                # OKX sends one subscribe confirmation per instrument. Do not log each one.
                return
            elif ev == "error":
                msg_txt = str(data.get("msg") or "")
                if msg_txt == "Illegal request: ping":
                    # public-sbe does not accept text ping. We disable app-level ping below,
                    # but ignore this if an old connection emitted one during rollout.
                    return
                log.warning("[OKX SBE] error: %s", data)
            else:
                log.debug("[OKX SBE] text message: %s", data)
            return

        if not isinstance(msg, (bytes, bytearray)):
            return

        recv_ns = time.perf_counter_ns()
        q = _decode_okx_sbe_bbo_tbt(bytes(msg), inst_code_map)
        if not q:
            unknown_count += 1
            if unknown_count % OKX_SBE_LOG_UNKNOWN_EVERY == 1:
                try:
                    header = struct.unpack_from("<HHHH", bytes(msg), 0) if len(msg) >= 8 else None
                except Exception:
                    header = None
                log.warning("[OKX SBE] unknown/undecodable binary len=%d header=%s", len(msg), header)
            return

        sym, bid, ask, bid_sz, ask_sz, ts_ns, seq = q
        engine.on_quote(
            "okx",
            sym,
            bid,
            ask,
            bid_sz,
            ask_sz,
            exch_ts_ns=ts_ns or recv_ns,
            recv_ts_ns=recv_ns,
            source_name="okx_sbe",
        )
        decoded_count += 1
        now = time.perf_counter()
        if OKX_SBE_SAMPLE_LOG_EVERY_SEC > 0 and now - last_decode_log >= float(OKX_SBE_SAMPLE_LOG_EVERY_SEC):
            log.info(
                "[OKX SBE] decoded=%d sample sym=%s bid=%.8f ask=%.8f bid_sz=%.4f ask_sz=%.4f ts_ns=%d seq=%d",
                decoded_count,
                sym,
                bid,
                ask,
                bid_sz,
                ask_sz,
                ts_ns,
                seq,
            )
            last_decode_log = now

    last_err: Optional[BaseException] = None
    for ws_url in OKX_SBE_WS_URLS:
        try:
            log.info("[OKX SBE] connecting url=%s mapped=%d/%d", ws_url, len(inst_code_map), len(engine.symbols))
            await _ws_loop(
                "okx",
                ws_url,
                sub,
                on_msg,
                stats=engine.conn_stats.get("okx"),
                notify_fn=getattr(engine, "_notify", None),
                # OKX public-sbe rejects text ping with code 60012.
                # Let the websockets library handle protocol-level ping/pong.
                ping_text=None,
                ping_every=0.0,
                ping_interval=20.0,
                on_disconnect_fn=lambda _name: engine.invalidate_exchange("okx"),
                extra_headers=_auth_headers,
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_err = e
            log.warning("[OKX SBE] url failed url=%s err=%r", ws_url, e)
            await asyncio.sleep(1.0)
    if last_err:
        raise last_err

async def okx_client(engine: Engine) -> None:
    url = "wss://ws.okx.com:8443/ws/v5/public"

    def _args() -> List[Dict[str, str]]:
        active = [s for s in engine.symbols if s not in engine.disabled.get("okx", set())]
        return [{"channel": "bbo-tbt", "instId": okx_inst_id(s)} for s in active]

    async def sub(ws):
        args = _args()
        for i in range(0, len(args), 50):
            await ws.send(_dumps({"op": "subscribe", "args": args[i:i + 50]}))

    async def on_msg(ws, msg: Any):
        data = _loads(_to_text(msg))
        if not isinstance(data, dict):
            return
        ev = data.get("event")
        if ev:
            if ev == "error":
                arg0 = data.get("arg") or {}
                inst0 = arg0.get("instId")
                if not inst0 and isinstance(data.get("msg"), str):
                    m = re.search(r"instId:([A-Z0-9\-]+)", data.get("msg"))
                    if m:
                        inst0 = m.group(1)
                if inst0:
                    sym0 = normalize_symbol(inst0.replace("-", "").removesuffix("SWAP"))
                    engine.disabled.setdefault("okx", set()).add(sym0)
                    try:
                        await sub(ws)
                    except Exception:
                        pass
            return
        arr = data.get("data")
        arg = data.get("arg") or {}
        inst = arg.get("instId")
        if not inst or not isinstance(arr, list) or not arr:
            return
        d0 = arr[0]
        bids = d0.get("bids") if isinstance(d0, dict) else None
        asks = d0.get("asks") if isinstance(d0, dict) else None
        if not (isinstance(bids, list) and bids and isinstance(asks, list) and asks):
            return
        try:
            bid = float(bids[0][0]); ask = float(asks[0][0])
            bid_sz = float(bids[0][1]) if len(bids[0]) > 1 else 0.0
            ask_sz = float(asks[0][1]) if len(asks[0]) > 1 else 0.0
        except Exception:
            return
        sym = normalize_symbol(inst.replace("-", "").removesuffix("SWAP"))
        engine.on_quote("okx", sym, bid, ask, bid_sz, ask_sz, int(d0.get("ts") or 0) * 1_000_000, source_name="okx")

    await _ws_loop("okx", url, sub, on_msg, stats=engine.conn_stats.get("okx"), notify_fn=getattr(engine, "_notify", None), ping_interval=20.0, on_disconnect_fn=lambda _name: engine.invalidate_exchange("okx"))


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
