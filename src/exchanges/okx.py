from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import struct
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from ..config import (
    OKX_API_KEY_PARSER, OKX_PASSWORD_PARSER, OKX_SBE_LOG_UNKNOWN_EVERY,
    OKX_SBE_SAMPLE_LOG_EVERY_SEC, OKX_SBE_SCHEMA_URL, OKX_SBE_WS_URLS,
    OKX_SECRET_PARSER,
)
from ..utils import normalize_symbol, okx_inst_id
from ..websocket import _dumps, _loads, _to_text, _ws_loop

if TYPE_CHECKING:
    from ..engine import Engine

log = logging.getLogger("spread_watcher")

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


