from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timezone
from typing import List

from .models import EpisodeEvent

log = logging.getLogger("spread_watcher")

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


