from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List

from dotenv import load_dotenv

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

OKX_USE_SBE = os.getenv("OKX_USE_SBE", "0").strip() == "1"
OKX_API_KEY_PARSER = (os.getenv("OKX_API_KEY_PARSER") or "").strip()
OKX_SECRET_PARSER = (os.getenv("OKX_SECRET_PARSER") or "").strip()
OKX_PASSWORD_PARSER = (os.getenv("OKX_PASSWORD_PARSER") or "").strip()
OKX_SBE_WS_URL = os.getenv("OKX_SBE_WS_URL", "wss://ws.okx.com:8443/ws/v5/public-sbe").strip()
OKX_SBE_WS_URLS = [x.strip() for x in os.getenv("OKX_SBE_WS_URLS", f"{OKX_SBE_WS_URL},wss://wsaws.okx.com:8443/ws/v5/public-sbe").split(",") if x.strip()]
OKX_SBE_SCHEMA_PATH = os.getenv("OKX_SBE_SCHEMA_PATH", "okx-sbe-schema.xml").strip()
OKX_SBE_SCHEMA_URL = os.getenv("OKX_SBE_SCHEMA_URL", "https://www.okx.com/docs-v5/log_en/xml/okx_sbe_1_0.xml").strip()
OKX_SBE_LOG_UNKNOWN_EVERY = max(1, _env_int("OKX_SBE_LOG_UNKNOWN_EVERY", 50000))
OKX_SBE_SAMPLE_LOG_EVERY_SEC = max(0, _env_int("OKX_SBE_SAMPLE_LOG_EVERY_SEC", 0))


ALL_EXCHANGES = ["binance", "okx", "bybit", "bitget"]
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


EXCHANGES = ACTIVE_EXCHANGES
EX2I = {name: i for i, name in enumerate(EXCHANGES)}

