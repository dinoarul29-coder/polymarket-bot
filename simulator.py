"""
Polymarket BTC 5-Minute Paper Trading Bot
=========================================
Runs as a Flask web service (Render free-tier compatible).
Background thread handles all market logic.
PAPER TRADING ONLY — no real orders, no authentication.
"""

import csv
import logging
import os
import sys
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Optional
import re as _re

from flask import Flask, jsonify, Response
import requests

# ──────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

CFG = {
    "port":                int(os.environ.get("PORT", 10000)),
    "poll_interval":       _env_float("POLL_INTERVAL", 8),
    "btc_lookback_s":      _env_float("BTC_LOOKBACK_S", 20),
    "min_move_pct":        _env_float("MIN_MOVE_PCT", 0.04),
    "arb_threshold":       _env_float("ARB_THRESHOLD", 0.97),
    "max_up_ask":          _env_float("MAX_UP_ASK", 0.45),
    "min_dn_ask":          _env_float("MIN_DN_ASK", 0.55),
    "max_spread":          _env_float("MAX_SPREAD", 0.03),
    "min_size":            _env_float("MIN_SIZE", 50),
    "shares":              _env_int("SHARES", 100),
    "profit_target":       _env_float("PROFIT_TARGET", 0.05),
    "fee_rate":            _env_float("FEE_RATE", 0.0),
    "entry_window_start":  _env_int("ENTRY_WINDOW_START_S", 90),
    "entry_window_end":    _env_int("ENTRY_WINDOW_END_S", 60),
    "log_file":            os.environ.get("LOG_FILE", "trades.csv"),
    "max_log_lines":       _env_int("MAX_LOG_LINES", 200),
}

# ──────────────────────────────────────────────────────────────────────────────
#  ENDPOINTS
# ──────────────────────────────────────────────────────────────────────────────

COINBASE_BTC_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
HTTP_TIMEOUT = 8

# ──────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.Formatter.converter = time.gmtime
log = logging.getLogger("bot")

_log_buffer = deque(maxlen=CFG["max_log_lines"])
_log_lock = threading.Lock()

class _BufferHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        with _log_lock:
            _log_buffer.append(msg)

_buf_handler = _BufferHandler()
_buf_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                            datefmt="%H:%M:%S"))
log.addHandler(_buf_handler)

# ──────────────────────────────────────────────────────────────────────────────
#  SHARED STATE
# ──────────────────────────────────────────────────────────────────────────────

_state_lock = threading.RLock()

state = {
    "status":          "INITIALIZING",
    "started_at":      datetime.now(tz=timezone.utc).isoformat(),
    "tick":            0,
    "last_tick_at":    None,

    "market_id":       None,
    "market_question": None,
    "market_expiry":   None,
    "token_id_up":     None,
    "token_id_dn":     None,
    "market_open_btc": None,

    "pending_market":  None,

    "btc_price":       None,
    "btc_history":     deque(maxlen=300),

    "up_quote":        None,
    "dn_quote":        None,

    "cooldowns":       {},

    "trades":          [],
    "trade_counter":   0,

    "net_pnl":         0.0,
    "equity_peak":     0.0,
    "max_drawdown":    0.0,
}

_rej = {"spread": 0, "size": 0, "cooldown": 0, "arb_blocks_dir": 0, "no_signal": 0}

# ──────────────────────────────────────────────────────────────────────────────
#  HTTP HELPER
# ──────────────────────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({"User-Agent": "polybot-paper/1.0", "Accept": "application/json"})

def _get(url: str, params: dict = None) -> Optional[dict]:
    try:
        r = _session.get(url, params=params, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.Timeout:
        log.warning(f"Timeout: {url}")
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", "?")
        log.warning(f"HTTP {code}: {url}")
    except requests.exceptions.RequestException as e:
        log.warning(f"Request error {url}: {e}")
    except Exception as e:
        log.warning(f"Unexpected error {url}: {e}")
    return None

# ──────────────────────────────────────────────────────────────────────────────
#  BTC FEED
# ──────────────────────────────────────────────────────────────────────────────

def fetch_btc_price() -> Optional[float]:
    data = _get(COINBASE_BTC_URL)
    if not data:
        return None
    try:
        return float(data["data"]["amount"])
    except (KeyError, TypeError, ValueError) as e:
        log.warning(f"BTC price parse error: {e} | raw={str(data)[:200]}")
        return None

def record_btc(price: float):
    ts_ms = int(time.time() * 1000)
    with _state_lock:
        state["btc_history"].append((ts_ms, price))
        state["btc_price"] = price

def compute_momentum() -> Optional[float]:
    lookback_ms = CFG["btc_lookback_s"] * 1000
    now_ms = time.time() * 1000
    cutoff = now_ms - lookback_ms
    with _state_lock:
        window = [(ts, px) for ts, px in state["btc_history"] if ts >= cutoff]
    if len(window) < 2:
        return None
    return (window[-1][1] - window[0][1]) / window[0][1]

# ──────────────────────────────────────────────────────────────────────────────
#  MARKET DISCOVERY
# ──────────────────────────────────────────────────────────────────────────────

_FIVEMIN_RE = _re.compile(
    r"\b5[\s\-]?min(?:ute)?s?\b|five[\s\-]?minutes?\b|\b5m\b",
    _re.IGNORECASE
)
_BTC_RE = _re.compile(r"\b(btc|bitcoin)\b", _re.IGNORECASE)

def _has_updown(text: str) -> bool:
    t = text.lower()
    has_up = any(k in t for k in ("up", "higher", "above", "rise"))
    has_down = any(k in t for k in ("down", "lower", "below", "fall"))
    return has_up and has_down

def _parse_expiry(obj: dict) -> Optional[float]:
    for key in ("endDate", "end_date", "expirationDate", "expiration", "closeTime", "endDateIso"):
        val = obj.get(key)
        if not val:
            continue
        try:
            if isinstance(val, (int, float)):
                ts = float(val)
                return ts / 1000.0 if ts > 1e12 else ts
            s = str(val).strip().replace("Z", "+00:00").replace(" ", "T")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            continue
    return None

def _extract_tokens(market: dict) -> Optional[tuple]:
    outcomes = market.get("outcomes") or []
    clob_ids = market.get("clobTokenIds") or market.get("tokens") or []

    if outcomes and isinstance(outcomes[0], dict):
        up = dn = None
        for o in outcomes:
            name = (o.get("outcome") or o.get("name") or o.get("title") or "").lower()
            if any(k in name for k in ("up", "higher", "above", "yes")):
                up = o
            elif any(k in name for k in ("down", "lower", "below", "no")):
                dn = o
        if up and dn:
            uid = up.get("token_id") or up.get("tokenId") or up.get("id")
            did = dn.get("token_id") or dn.get("tokenId") or dn.get("id")
            if uid and did:
                return (up.get("outcome", "Up"), dn.get("outcome", "Down"), str(uid), str(did))

    if isinstance(outcomes, list) and isinstance(clob_ids, list) and len(outcomes) >= 2 and len(clob_ids) >= 2:
        up_idx = dn_idx = None
        for i, name in enumerate(outcomes):
            n = str(name).lower()
            if any(k in n for k in ("up", "higher", "above", "yes")):
                up_idx = i
            elif any(k in n for k in ("down", "lower", "below", "no")):
                dn_idx = i
        if up_idx is not None and dn_idx is not None:
            return (
                str(outcomes[up_idx]),
                str(outcomes[dn_idx]),
                str(clob_ids[up_idx]),
                str(clob_ids[dn_idx]),
            )

    return None

def discover_market() -> Optional[dict]:
    log.info("Market discovery — querying Gamma markets API")

    all_markets = []
    offset = 0
    limit = 200
    now = time.time()

    while offset < 1000:
        params = {
            "active": "true",
            "closed": "false",
            "limit": limit,
            "offset": offset,
        }
        data = _get(GAMMA_MARKETS_URL, params=params)
        if data is None:
            log.warning("Gamma markets API unreachable")
            return None

        markets = data if isinstance(data, list) else (
            data.get("data") or data.get("markets") or data.get("results") or []
        )

        if not markets:
            break

        all_markets.extend(markets)

        if len(markets) < limit:
            break

        offset += limit

    log.info(f"Gamma returned {len(all_markets)} markets")

    candidates = []
    near_misses = []

    for mkt in all_markets:
        question = str(mkt.get("question") or mkt.get("title") or "")
        desc = str(mkt.get("description") or "")
        slug = str(mkt.get("slug") or "")
        group_title = str(mkt.get("groupItemTitle") or "")
        combined = f"{question} {desc} {slug} {group_title}".lower()

        slug_match = "btc-updown-5m" in slug
        is_btc = bool(_BTC_RE.search(combined))
        is_5m = bool(_FIVEMIN_RE.search(combined)) or slug_match
        is_updown = _has_updown(combined) or slug_match

        label_for_log = question or slug or "unknown"

        if not is_btc:
            continue
        if not is_5m:
            if len(near_misses) < 10:
                near_misses.append(f"SKIP no 5m: {label_for_log}")
            continue
        if not is_updown:
            if len(near_misses) < 10:
                near_misses.append(f"SKIP no up/down: {label_for_log}")
            continue
        if mkt.get("closed") or mkt.get("archived") or mkt.get("resolved"):
            if len(near_misses) < 10:
                near_misses.append(f"SKIP closed/resolved: {label_for_log}")
            continue
        if mkt.get("enableOrderBook") is False:
            if len(near_misses) < 10:
                near_misses.append(f"SKIP no orderbook: {label_for_log}")
            continue

        expiry = _parse_expiry(mkt)
        if expiry is None or expiry <= now:
            if len(near_misses) < 10:
                near_misses.append(f"SKIP bad expiry: {label_for_log}")
            continue

        token_info = _extract_tokens(mkt)
        if token_info is None:
            if len(near_misses) < 10:
                near_misses.append(f"SKIP no tokens: {label_for_log}")
            continue

        up_label, dn_label, up_tid, dn_tid = token_info

        fee_rate = 0.0
        for key in ("feeRate", "makerBaseFee", "takerBaseFee", "fee_rate"):
            val = mkt.get(key)
            if val is not None:
                try:
                    fee_rate = float(val)
                    if fee_rate > 0:
                        break
                except (TypeError, ValueError):
                    pass

        candidate = {
            "market_id":   str(mkt.get("id") or mkt.get("conditionId") or slug or "?"),
            "question":    label_for_log,
            "expiry_ts":   expiry,
            "up_label":    up_label,
            "dn_label":    dn_label,
            "token_id_up": up_tid,
            "token_id_dn": dn_tid,
            "fee_rate":    fee_rate,
        }
        candidates.append(candidate)
        log.info(
            f"CANDIDATE id={candidate['market_id']} "
            f"expiry_in={expiry - now:.0f}s "
            f"slug='{slug}' q='{candidate['question'][:80]}'"
        )

    for line in near_misses[:10]:
        log.info(line)

    if not candidates:
        log.warning("No BTC 5m Up/Down market found in Gamma markets")
        return None

    valid = [c for c in candidates if c["expiry_ts"] - now > 25]
    if not valid:
        log.warning("All candidates expire too soon")
        return None

    best = min(valid, key=lambda c: c["expiry_ts"])
    log.info(f"Selected market: {best['market_id']} | {best['question'][:70]}")
    return best

# ──────────────────────────────────────────────────────────────────────────────
#  CLOB ORDER BOOK
# ──────────────────────────────────────────────────────────────────────────────

def fetch_book(token_id: str) -> dict:
    dead = {
        "live": False,
        "ask": None,
        "bid": None,
        "spread": None,
        "ask_size": 0.0,
        "bid_size": 0.0,
        "mid": None,
        "token_id": token_id,
    }
    data = _get(CLOB_BOOK_URL, params={"token_id": token_id})
    if data is None:
        return dead
    try:
        asks_raw = data.get("asks") or []
        bids_raw = data.get("bids") or []

        asks = sorted(
            [{"p": float(a["price"]), "s": float(a["size"])} for a in asks_raw if "price" in a and "size" in a],
            key=lambda x: x["p"]
        )
        bids = sorted(
            [{"p": float(b["price"]), "s": float(b["size"])} for b in bids_raw if "price" in b and "size" in b],
            key=lambda x: -x["p"]
        )

        best_ask = asks[0]["p"] if asks else None
        ask_size = asks[0]["s"] if asks else 0.0
        best_bid = bids[0]["p"] if bids else None
        bid_size = bids[0]["s"] if bids else 0.0
        spread = round(best_ask - best_bid, 6) if (best_ask is not None and best_bid is not None) else None
        mid = round((best_ask + best_bid) / 2, 6) if (best_ask is not None and best_bid is not None) else None

        return {
            "live": True,
            "ask": best_ask,
            "bid": best_bid,
            "spread": spread,
            "ask_size": ask_size,
            "bid_size": bid_size,
            "mid": mid,
            "token_id": token_id,
        }
    except Exception as e:
        log.warning(f"CLOB parse error {token_id[:16]}: {e}")
        return dead

# ──────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _window_key() -> Optional[str]:
    with _state_lock:
        mid = state["market_id"]
        exp = state["market_expiry"]
    if mid and exp:
        return f"{mid}:{int(exp)}"
    return None

def _in_cooldown(wk: str, signal: str) -> bool:
    with _state_lock:
        return signal in state["cooldowns"].get(wk, set())

def _set_cooldown(wk: str, signal: str):
    with _state_lock:
        state["cooldowns"].setdefault(wk, set()).add(signal)

def _arb_entered(wk: str) -> bool:
    with _state_lock:
        return "arb" in state["cooldowns"].get(wk, set())

def seconds_to_expiry() -> Optional[float]:
    with _state_lock:
        exp = state["market_expiry"]
    return (exp - time.time()) if exp else None

def in_entry_window() -> bool:
    ste = seconds_to_expiry()
    if ste is None:
        return False
    return CFG["entry_window_end"] <= ste <= CFG["entry_window_start"]

def _fee(price: float, shares: int) -> float:
    rate = CFG["fee_rate"]
    return round(rate * price * shares, 6) if rate > 0 else 0.0

def _utcnow() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ──────────────────────────────────────────────────────────────────────────────
#  CSV LOGGING
# ──────────────────────────────────────────────────────────────────────────────

_CSV_FIELDS = [
    "event", "id", "timestamp", "market_id", "window_key", "signal", "side",
    "time_remaining_s", "btc_entry", "market_open_btc", "momentum_pct",
    "entry_ask", "entry_ask_up", "entry_ask_dn", "avail_size", "shares",
    "entry_fee", "exit_fee", "exit_price", "exit_reason", "resolution", "pnl",
    "btc_exit", "status",
]
_csv_lock = threading.Lock()
_csv_initialized = False

def _ensure_csv():
    global _csv_initialized
    with _csv_lock:
        if _csv_initialized:
            return
        path = CFG["log_file"]
        write_header = not os.path.exists(path) or os.path.getsize(path) == 0
        try:
            with open(path, "a", newline="", encoding="utf-8") as f:
                if write_header:
                    csv.DictWriter(f, fieldnames=_CSV_FIELDS).writeheader()
            _csv_initialized = True
        except Exception as e:
            log.warning(f"CSV init failed: {e}")

def _log_trade_csv(trade: dict, event: str):
    _ensure_csv()
    row = {k: trade.get(k) for k in _CSV_FIELDS}
    row["event"] = event
    with _csv_lock:
        try:
            with open(CFG["log_file"], "a", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore").writerow(row)
        except Exception as e:
            log.warning(f"CSV write failed: {e}")

# ──────────────────────────────────────────────────────────────────────────────
#  TRADE ENTRY
# ──────────────────────────────────────────────────────────────────────────────

def _enter_trade(signal: str, side: str, entry_ask: float,
                 up_ask: Optional[float], dn_ask: Optional[float],
                 avail_size: float, btc_price: float, momentum: Optional[float],
                 time_left: float):
    shares = CFG["shares"]
    entry_fee = _fee(entry_ask, shares)
    wk = _window_key()

    with _state_lock:
        state["trade_counter"] += 1
        tid = state["trade_counter"]
        mkt_open = state["market_open_btc"] or btc_price

        trade = {
            "id":               tid,
            "window_key":       wk,
            "market_id":        state["market_id"],
            "token_id_up":      state["token_id_up"],
            "token_id_dn":      state["token_id_dn"],
            "signal":           signal,
            "side":             side,
            "timestamp":        _utcnow(),
            "time_remaining_s": round(time_left, 1),
            "btc_entry":        btc_price,
            "market_open_btc":  mkt_open,
            "momentum_pct":     round(momentum * 100, 5) if momentum is not None else None,
            "entry_ask":        round(entry_ask, 6),
            "entry_ask_up":     round(up_ask, 6) if up_ask is not None else None,
            "entry_ask_dn":     round(dn_ask, 6) if dn_ask is not None else None,
            "avail_size":       avail_size,
            "shares":           shares,
            "entry_fee":        entry_fee,
            "exit_fee":         0.0,
            "exit_price":       None,
            "exit_reason":      None,
            "btc_exit":         None,
            "resolution":       None,
            "pnl":              None,
            "status":           "OPEN",
        }
        state["trades"].append(trade)

    arb_str = (f" up={up_ask:.4f}+dn={dn_ask:.4f}={entry_ask:.4f}"
               if signal == "arb" and up_ask is not None and dn_ask is not None
               else f" ask={entry_ask:.4f}")
    mom_str = f"{momentum * 100:+.4f}%" if momentum is not None else "N/A"
    log.info(f">>> TRADE #{tid} ENTERED | {signal.upper()} {side}{arb_str} | "
             f"shares={shares} fee=${entry_fee:.4f} | "
             f"btc=${btc_price:.2f} mom={mom_str} | {time_left:.0f}s left")
    _log_trade_csv(trade, "ENTER")
    return trade

# ──────────────────────────────────────────────────────────────────────────────
#  STRATEGY
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_and_enter():
    with _state_lock:
        up_q = state["up_quote"]
        dn_q = state["dn_quote"]
        btc = state["btc_price"]

    if not up_q or not dn_q or not btc:
        return
    if not up_q.get("live") or not dn_q.get("live"):
        return

    wk = _window_key()
    if not wk:
        return

    min_size = max(float(CFG["min_size"]), float(CFG["shares"]))
    mom = compute_momentum()
    ste = seconds_to_expiry()
    if ste is None:
        return

    up_ask = up_q["ask"]
    dn_ask = dn_q["ask"]
    up_spread = up_q["spread"]
    dn_spread = dn_q["spread"]
    up_sz = up_q["ask_size"]
    dn_sz = dn_q["ask_size"]
    max_spread = CFG["max_spread"]

    spread_ok_up = up_spread is not None and up_spread <= max_spread
    spread_ok_dn = dn_spread is not None and dn_spread <= max_spread

    if up_ask is not None and dn_ask is not None:
        arb_sum = round(up_ask + dn_ask, 6)
        if arb_sum <= CFG["arb_threshold"]:
            if _in_cooldown(wk, "arb"):
                _rej["cooldown"] += 1
            elif not (spread_ok_up and spread_ok_dn):
                log.info(f"ARB REJECTED [spread]: up={up_spread} dn={dn_spread} max={max_spread}")
                _rej["spread"] += 1
            elif up_sz < min_size or dn_sz < min_size:
                log.info(f"ARB REJECTED [size]: up={up_sz:.0f} dn={dn_sz:.0f} need={min_size}")
                _rej["size"] += 1
            else:
                _enter_trade("arb", "UP+DOWN", arb_sum, up_ask, dn_ask,
                             min(up_sz, dn_sz), btc, mom, ste)
                _set_cooldown(wk, "arb")
                return

    if _arb_entered(wk):
        _rej["arb_blocks_dir"] += 1
        return

    if mom is None:
        return

    min_move = CFG["min_move_pct"] / 100.0

    if mom >= min_move and up_ask is not None and up_ask <= CFG["max_up_ask"]:
        if _in_cooldown(wk, "momentum_UP"):
            _rej["cooldown"] += 1
        elif not spread_ok_up:
            log.info(f"UP REJECTED [spread]: {up_spread:.4f} > {max_spread}")
            _rej["spread"] += 1
        elif up_sz < min_size:
            log.info(f"UP REJECTED [size]: {up_sz:.0f} < {min_size}")
            _rej["size"] += 1
        else:
            _enter_trade("momentum", "UP", up_ask, None, None, up_sz, btc, mom, ste)
            _set_cooldown(wk, "momentum_UP")
        return

    if mom <= -min_move and dn_ask is not None and dn_ask >= CFG["min_dn_ask"]:
        if _in_cooldown(wk, "momentum_DOWN"):
            _rej["cooldown"] += 1
        elif not spread_ok_dn:
            log.info(f"DN REJECTED [spread]: {dn_spread:.4f} > {max_spread}")
            _rej["spread"] += 1
        elif dn_sz < min_size:
            log.info(f"DN REJECTED [size]: {dn_sz:.0f} < {min_size}")
            _rej["size"] += 1
        else:
            _enter_trade("momentum", "DOWN", dn_ask, None, None, dn_sz, btc, mom, ste)
            _set_cooldown(wk, "momentum_DOWN")
        return

    _rej["no_signal"] += 1

# ──────────────────────────────────────────────────────────────────────────────
#  EXITS
# ──────────────────────────────────────────────────────────────────────────────

def _close_trade_direct(trade: dict, exit_price: float, reason: str,
                        resolution: str, pnl: float, btc_exit: float, exit_fee: float):
    trade["exit_price"] = exit_price
    trade["exit_reason"] = reason
    trade["resolution"] = resolution
    trade["pnl"] = pnl
    trade["btc_exit"] = btc_exit
    trade["exit_fee"] = exit_fee
    trade["status"] = "CLOSED"

    with _state_lock:
        state["net_pnl"] = round(state["net_pnl"] + (pnl or 0), 4)
        state["equity_peak"] = max(state["equity_peak"], state["net_pnl"])
        dd = state["equity_peak"] - state["net_pnl"]
        state["max_drawdown"] = max(state["max_drawdown"], dd)

    log.info(f"<<< TRADE #{trade['id']} CLOSED | {resolution} | {trade['side']} | "
             f"exit={exit_price:.4f} pnl=${pnl:.4f} via {reason}")
    _log_trade_csv(trade, "CLOSE")

def _close_trade(trade: dict, exit_price: float, reason: str, btc_exit: float, fee_rate: float):
    exit_fee = _fee(exit_price, trade["shares"])
    pnl = round((exit_price - trade["entry_ask"]) * trade["shares"]
                - trade["entry_fee"] - exit_fee, 4)
    resolution = "WIN" if pnl > 0 else "LOSS"
    _close_trade_direct(trade, exit_price, reason, resolution, pnl, btc_exit, exit_fee)

def check_exits():
    with _state_lock:
        up_q = state["up_quote"]
        dn_q = state["dn_quote"]
        btc = state["btc_price"]
        trades = list(state["trades"])
        mkt_id = state["market_id"]

    if not btc:
        return

    for trade in trades:
        if trade["status"] != "OPEN" or trade["signal"] == "arb":
            continue
        if trade["market_id"] != mkt_id:
            continue

        q = up_q if trade["side"] == "UP" else dn_q
        if not q or not q.get("live"):
            continue

        bid = q.get("bid")
        bid_sz = q.get("bid_size", 0)
        shares = trade["shares"]

        if bid is None or bid_sz < shares:
            continue

        gain = bid - trade["entry_ask"]
        if gain >= CFG["profit_target"] - 1e-9:
            _close_trade(trade, bid, "profit_target", btc, CFG["fee_rate"])

def resolve_expired_trades():
    with _state_lock:
        trades = list(state["trades"])
        btc = state["btc_price"]
        mkt_id = state["market_id"]

    if not btc:
        return

    for trade in trades:
        if trade["status"] != "OPEN":
            continue
        if trade["market_id"] != mkt_id:
            continue

        if trade["signal"] == "arb":
            pnl = round((1.0 - trade["entry_ask"]) * trade["shares"] - trade["entry_fee"], 4)
            _close_trade_direct(
                trade,
                exit_price=1.0,
                reason="expiry",
                resolution="APPROX_WIN" if pnl >= 0 else "APPROX_LOSS",
                pnl=pnl,
                btc_exit=btc,
                exit_fee=0.0,
            )
        else:
            mkt_open = trade["market_open_btc"] or trade["btc_entry"]
            btc_up = btc >= mkt_open
            side_wins = ((trade["side"] == "UP" and btc_up) or
                         (trade["side"] == "DOWN" and not btc_up))
            exit_px = 1.0 if side_wins else 0.0
            pnl = round((exit_px - trade["entry_ask"]) * trade["shares"] - trade["entry_fee"], 4)
            res = "APPROX_WIN" if side_wins else "APPROX_LOSS"
            _close_trade_direct(trade, exit_px, "expiry", res, pnl, btc, 0.0)

# ──────────────────────────────────────────────────────────────────────────────
#  ROLLOVER
# ──────────────────────────────────────────────────────────────────────────────

def _open_trades_for_current_market() -> list:
    with _state_lock:
        mkt_id = state["market_id"]
        return [t for t in state["trades"] if t["status"] == "OPEN" and t["market_id"] == mkt_id]

def _commit_market(mkt: dict):
    with _state_lock:
        old = state["market_id"]
        state["market_id"] = mkt["market_id"]
        state["market_question"] = mkt["question"]
        state["market_expiry"] = mkt["expiry_ts"]
        state["token_id_up"] = mkt["token_id_up"]
        state["token_id_dn"] = mkt["token_id_dn"]
        state["market_open_btc"] = None
        state["pending_market"] = None
        state["up_quote"] = None
        state["dn_quote"] = None
    log.info(f"[MARKET COMMITTED] {old} → {mkt['market_id']} "
             f"expiry_in={mkt['expiry_ts'] - time.time():.0f}s")

def handle_market_rollover():
    with _state_lock:
        pending = state.get("pending_market")

    if pending:
        open_old = _open_trades_for_current_market()
        if not open_old:
            log.info("[ROLLOVER] Old trades all closed — committing pending market")
            _commit_market(pending)
        return

    mkt = discover_market()
    if mkt is None:
        with _state_lock:
            state["status"] = "NO_MARKET"
        return

    with _state_lock:
        current_id = state["market_id"]

    if mkt["market_id"] == current_id:
        return

    open_old = _open_trades_for_current_market()

    if current_id is None or not open_old:
        _commit_market(mkt)
    else:
        log.info(f"[ROLLOVER] Resolving {len(open_old)} open trade(s) before swap")
        resolve_expired_trades()
        with _state_lock:
            state["pending_market"] = mkt
        log.info(f"[ROLLOVER STAGED] Pending: {mkt['market_id']}")

# ──────────────────────────────────────────────────────────────────────────────
#  METRICS
# ──────────────────────────────────────────────────────────────────────────────

def build_metrics() -> dict:
    with _state_lock:
        trades = list(state["trades"])
        net_pnl = state["net_pnl"]
        max_dd = state["max_drawdown"]

    closed = [t for t in trades if t["status"] == "CLOSED" and t["pnl"] is not None]
    open_t = [t for t in trades if t["status"] == "OPEN"]
    wins = [t for t in closed if t["pnl"] > 0]
    losses = [t for t in closed if t["pnl"] <= 0]

    gross_win = sum(t["pnl"] for t in wins)
    gross_loss = abs(sum(t["pnl"] for t in losses))
    pf = round(gross_win / gross_loss, 3) if gross_loss > 0 else (float("inf") if gross_win > 0 else 0.0)
    wr = round(len(wins) / len(closed) * 100, 1) if closed else 0.0
    avg_win = round(gross_win / len(wins), 4) if wins else 0.0
    avg_loss = round(-gross_loss / len(losses), 4) if losses else 0.0

    dir_closed = [t for t in closed if t["signal"] == "momentum"]
    arb_closed = [t for t in closed if t["signal"] == "arb"]

    return {
        "total_trades":    len(trades),
        "open_trades":     len(open_t),
        "closed_trades":   len(closed),
        "win_rate_pct":    wr,
        "avg_win":         avg_win,
        "avg_loss":        avg_loss,
        "net_pnl":         round(net_pnl, 4),
        "max_drawdown":    round(max_dd, 4),
        "profit_factor":   pf,
        "directional_pnl": round(sum(t["pnl"] for t in dir_closed), 4),
        "arb_pnl":         round(sum(t["pnl"] for t in arb_closed), 4),
        "rejections":      dict(_rej),
    }

# ──────────────────────────────────────────────────────────────────────────────
#  BOT LOOP
# ──────────────────────────────────────────────────────────────────────────────

_DISCOVERY_INTERVAL = 60
_last_discovery = 0.0

def bot_tick():
    global _last_discovery

    now = time.time()
    with _state_lock:
        state["tick"] += 1
        state["last_tick_at"] = _utcnow()

    btc = fetch_btc_price()
    if btc is not None:
        record_btc(btc)
        with _state_lock:
            if state["market_open_btc"] is None and state["market_id"]:
                state["market_open_btc"] = btc
                log.info(f"Market open BTC recorded: ${btc:.2f}")
    else:
        with _state_lock:
            state["status"] = "NO_BTC"
        log.warning("BTC fetch failed this tick")

    with _state_lock:
        market_id = state["market_id"]
        exp = state["market_expiry"]

    market_expired = exp is not None and exp - now < 10

    if market_expired or market_id is None or (now - _last_discovery) > _DISCOVERY_INTERVAL:
        if market_expired:
            log.info(f"Market {market_id} expired — resolving open trades")
            resolve_expired_trades()
        handle_market_rollover()
        _last_discovery = now

    with _state_lock:
        up_tid = state["token_id_up"]
        dn_tid = state["token_id_dn"]
        mkt_id = state["market_id"]

    if up_tid and dn_tid:
        up_q = fetch_book(up_tid)
        dn_q = fetch_book(dn_tid)
        with _state_lock:
            state["up_quote"] = up_q
            state["dn_quote"] = dn_q

        both_live = up_q.get("live") and dn_q.get("live")
        if not both_live:
            with _state_lock:
                state["status"] = "BLOCKED"
            log.warning("CLOB quotes unavailable — trading suspended")
        elif btc is not None:
            with _state_lock:
                state["status"] = "LIVE"

        check_exits()

        ste = seconds_to_expiry()
        if ste is not None and both_live and btc is not None and in_entry_window():
            log.info(f"ENTRY WINDOW OPEN | market={mkt_id} ste={ste:.0f}s btc=${btc:.2f}")
            evaluate_and_enter()

    elif mkt_id is None:
        with _state_lock:
            state["status"] = "NO_MARKET"

def bot_loop():
    log.info("Bot background thread starting")
    time.sleep(3)

    while True:
        try:
            bot_tick()
        except Exception:
            log.error(f"Unhandled exception in bot tick:\n{traceback.format_exc()}")
        time.sleep(CFG["poll_interval"])

# ──────────────────────────────────────────────────────────────────────────────
#  FLASK APP
# ──────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "ts": _utcnow()}), 200

@app.route("/status")
def status():
    with _state_lock:
        s = {
            "status":           state["status"],
            "started_at":       state["started_at"],
            "last_tick_at":     state["last_tick_at"],
            "tick":             state["tick"],
            "market_id":        state["market_id"],
            "market_question":  state["market_question"],
            "market_expiry":    (datetime.fromtimestamp(state["market_expiry"], tz=timezone.utc).isoformat()
                                 if state["market_expiry"] else None),
            "seconds_to_expiry": round(seconds_to_expiry() or 0, 1),
            "token_id_up":      state["token_id_up"],
            "token_id_dn":      state["token_id_dn"],
            "btc_price":        state["btc_price"],
            "market_open_btc":  state["market_open_btc"],
            "up_quote":         state["up_quote"],
            "dn_quote":         state["dn_quote"],
            "in_entry_window":  in_entry_window(),
        }
    metrics = build_metrics()
    cfg_safe = {k: v for k, v in CFG.items() if k != "log_file"}
    return jsonify({"state": s, "metrics": metrics, "config": cfg_safe}), 200

@app.route("/logs")
def logs():
    with _log_lock:
        lines = list(_log_buffer)
    return Response("\n".join(lines), mimetype="text/plain")

@app.route("/")
def index():
    with _state_lock:
        status_val = state["status"]
        btc = state["btc_price"]
        question = state["market_question"] or "—"
        mkt_id = state["market_id"] or "—"
        expiry = state["market_expiry"]
        ste = seconds_to_expiry()
        up_q = state["up_quote"] or {}
        dn_q = state["dn_quote"] or {}
        tick = state["tick"]
        last_tick = state["last_tick_at"] or "—"
        net_pnl = state["net_pnl"]
        max_dd = state["max_drawdown"]

    metrics = build_metrics()
    expiry_str = (datetime.fromtimestamp(expiry, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                  if expiry else "—")
    ste_str = f"{ste:.0f}s" if ste is not None else "—"

    status_color = {
        "LIVE":      "#00d17a",
        "NO_MARKET": "#f59e0b",
        "NO_BTC":    "#f59e0b",
        "BLOCKED":   "#ff4d6a",
    }.get(status_val, "#8a97a3")

    def q_row(label: str, q: dict) -> str:
        live = "✓" if q.get("live") else "✗"
        ask = f"{q['ask']:.4f}" if q.get("ask") is not None else "—"
        bid = f"{q['bid']:.4f}" if q.get("bid") is not None else "—"
        spr = f"{q['spread']:.4f}" if q.get("spread") is not None else "—"
        asz = f"{q.get('ask_size', 0):.0f}"
        bsz = f"{q.get('bid_size', 0):.0f}"
        return (f"<tr><td>{label}</td><td>{live}</td>"
                f"<td>{ask}</td><td>{bid}</td><td>{spr}</td>"
                f"<td>{asz}</td><td>{bsz}</td></tr>")

    recent_trades = []
    with _state_lock:
        for t in reversed(state["trades"][-10:]):
            pnl_str = f"${t['pnl']:.4f}" if t["pnl"] is not None else "—"
            clr = "#00d17a" if (t["pnl"] or 0) > 0 else "#ff4d6a" if (t["pnl"] or 0) < 0 else "#8a97a3"
            recent_trades.append(
                f"<tr>"
                f"<td>#{t['id']}</td>"
                f"<td>{t['signal']}</td>"
                f"<td>{t['side']}</td>"
                f"<td>{t['entry_ask']:.4f}</td>"
                f"<td style='color:{clr}'>{pnl_str}</td>"
                f"<td>{t['status']}</td>"
                f"<td style='font-size:10px'>{t['timestamp']}</td>"
                f"</tr>"
            )
    trades_html = "\n".join(recent_trades) if recent_trades else "<tr><td colspan='7' style='color:#4d5c68;text-align:center'>No trades yet</td></tr>"

    rej = metrics["rejections"]

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="10">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Polymarket Paper Bot</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0a0c0e;color:#e2e8ed;font-family:monospace;font-size:13px;line-height:1.5;padding:0}}
  .header{{background:#111416;border-bottom:1px solid #252c32;padding:16px 24px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}}
  .header h1{{font-size:16px;font-weight:600;color:#e2e8ed}}
  .pill{{font-size:10px;padding:3px 10px;border-radius:2px;font-weight:600;border:1px solid currentColor}}
  .pill-paper{{color:#f59e0b;background:rgba(245,158,11,.1)}}
  .status-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;background:{status_color}}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;padding:20px 24px}}
  .card{{background:#111416;border:1px solid #252c32;border-radius:4px;padding:14px 16px}}
  .card-label{{font-size:9px;letter-spacing:.12em;color:#4d5c68;text-transform:uppercase;margin-bottom:6px}}
  .card-value{{font-size:20px;font-weight:600;color:#e2e8ed}}
  .card-value.pos{{color:#00d17a}}
  .card-value.neg{{color:#ff4d6a}}
  .section{{padding:0 24px 20px}}
  .section-title{{font-size:9px;letter-spacing:.14em;color:#4d5c68;text-transform:uppercase;margin-bottom:10px;padding-top:4px}}
  table{{width:100%;border-collapse:collapse;font-size:11px}}
  th{{background:#181c1f;padding:6px 10px;text-align:left;font-size:9px;color:#4d5c68;text-transform:uppercase;border-bottom:1px solid #252c32}}
  td{{padding:6px 10px;border-bottom:1px solid #181c1f;color:#8a97a3}}
  .mkt-box{{background:#111416;border:1px solid #252c32;border-radius:4px;padding:12px 16px;margin-bottom:12px;font-size:11px;color:#8a97a3}}
  .mkt-q{{font-size:13px;color:#e2e8ed;margin-bottom:6px;font-weight:500}}
  .info-row{{display:flex;gap:24px;flex-wrap:wrap;margin-top:4px}}
  .info-item{{color:#4d5c68;font-size:10px}}
  .info-val{{color:#8a97a3}}
  .rej-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px}}
  .rej-item{{background:#111416;border:1px solid #252c32;border-radius:3px;padding:8px 12px;font-size:10px}}
  .rej-label{{color:#4d5c68;margin-bottom:2px}}
  .rej-val{{font-size:14px;font-weight:600;color:#f59e0b}}
  .footer{{padding:12px 24px;border-top:1px solid #181c1f;font-size:10px;color:#4d5c68;display:flex;gap:20px}}
</style>
</head>
<body>
<div class="header">
  <span class="status-dot"></span>
  <h1>Polymarket Paper Bot</h1>
  <span class="pill pill-paper">PAPER ONLY</span>
  <span style="color:{status_color};font-size:11px;font-weight:600">{status_val}</span>
  <span style="margin-left:auto;color:#4d5c68;font-size:10px">Auto-refresh 10s</span>
</div>

<div class="grid">
  <div class="card"><div class="card-label">Net PnL</div><div class="card-value {'pos' if net_pnl >= 0 else 'neg'}">${net_pnl:.2f}</div></div>
  <div class="card"><div class="card-label">Win Rate</div><div class="card-value {'pos' if metrics['win_rate_pct'] >= 50 else 'neg'}">{metrics['win_rate_pct']:.1f}%</div></div>
  <div class="card"><div class="card-label">Total Trades</div><div class="card-value">{metrics['total_trades']}</div></div>
  <div class="card"><div class="card-label">Open / Closed</div><div class="card-value">{metrics['open_trades']} / {metrics['closed_trades']}</div></div>
  <div class="card"><div class="card-label">Max Drawdown</div><div class="card-value neg">${max_dd:.2f}</div></div>
  <div class="card"><div class="card-label">Profit Factor</div><div class="card-value {'pos' if metrics['profit_factor'] >= 1 else 'neg'}">{metrics['profit_factor']}</div></div>
  <div class="card"><div class="card-label">BTC Price</div><div class="card-value" style="font-size:17px">${f"{btc:,.2f}" if btc else "—"}</div></div>
  <div class="card"><div class="card-label">Tick #{tick}</div><div class="card-value" style="font-size:11px;padding-top:4px;color:#4d5c68">{last_tick}</div></div>
</div>

<div class="section">
  <div class="section-title">Active Market</div>
  <div class="mkt-box">
    <div class="mkt-q">{question}</div>
    <div class="info-row">
      <span class="info-item">ID: <span class="info-val">{mkt_id}</span></span>
      <span class="info-item">Expiry: <span class="info-val">{expiry_str}</span></span>
      <span class="info-item">Time left: <span class="info-val">{ste_str}</span></span>
      <span class="info-item">Entry window: <span class="info-val">{'OPEN ✓' if in_entry_window() else 'closed'}</span></span>
    </div>
  </div>

  <div class="section-title">Order Book</div>
  <table>
    <thead><tr><th>Side</th><th>Live</th><th>Ask</th><th>Bid</th><th>Spread</th><th>Ask Sz</th><th>Bid Sz</th></tr></thead>
    <tbody>
      {q_row("UP", up_q)}
      {q_row("DOWN", dn_q)}
    </tbody>
  </table>
</div>

<div class="section">
  <div class="section-title">Recent Trades (last 10)</div>
  <table>
    <thead><tr><th>#</th><th>Signal</th><th>Side</th><th>Entry</th><th>PnL</th><th>Status</th><th>Timestamp</th></tr></thead>
    <tbody>{trades_html}</tbody>
  </table>
</div>

<div class="section">
  <div class="section-title">Rejection Counters</div>
  <div class="rej-grid">
    <div class="rej-item"><div class="rej-label">Spread</div><div class="rej-val">{rej['spread']}</div></div>
    <div class="rej-item"><div class="rej-label">Size</div><div class="rej-val">{rej['size']}</div></div>
    <div class="rej-item"><div class="rej-label">Cooldown</div><div class="rej-val">{rej['cooldown']}</div></div>
    <div class="rej-item"><div class="rej-label">Arb blocks dir</div><div class="rej-val">{rej['arb_blocks_dir']}</div></div>
    <div class="rej-item"><div class="rej-label">No signal</div><div class="rej-val">{rej['no_signal']}</div></div>
  </div>
</div>

<div class="footer">
  <span>PAPER TRADING ONLY</span>
  <span>Logs: <a href="/logs" style="color:#3d9eff">/logs</a></span>
  <span>JSON: <a href="/status" style="color:#3d9eff">/status</a></span>
</div>
</body></html>"""
    return html, 200

# ──────────────────────────────────────────────────────────────────────────────
#  STARTUP
# ──────────────────────────────────────────────────────────────────────────────

def start_bot():
    t = threading.Thread(target=bot_loop, name="bot-loop", daemon=True)
    t.start()
    log.info("Bot thread started (daemon=True)")
    return t

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("  POLYMARKET BTC 5M PAPER TRADING BOT")
    log.info("  PAPER ONLY — NO REAL ORDERS")
    log.info("=" * 60)
    log.info("Config:")
    for k, v in CFG.items():
        log.info(f"  {k:<28} = {v}")

    _ensure_csv()
    start_bot()

    port = CFG["port"]
    log.info(f"Flask starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)
