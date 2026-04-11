#!/usr/bin/env python3
"""
============================================================
  Alpaca Momentum Bot  —  Web Dashboard
============================================================
  SETUP:
    pip install fastapi uvicorn alpaca-py pandas numpy yfinance
    python alpaca_dashboard.py
  Then open: http://localhost:8888
============================================================
"""

import asyncio, csv, json, logging, math, os, sys, threading, time, contextlib

# ── Sub-modules (split from monolith) ──────────────────────
import signals as _sig
import trending as _trend
import finnhub_stream as _fh
try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False
from collections import deque
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from config import DEFAULT_CONFIG, load_config, save_config
from state import BotState, _sanitize_floats
from alpaca_api import (
    connect_alpaca,
    fetch_bars as _api_fetch_bars,
    fetch_bars_batch as _api_fetch_bars_batch,
    get_latest_trade_price as _api_get_latest_trade_price,
    get_latest_trade_prices as _api_get_latest_trade_prices,
    close_position as _api_close_position,
    _get_feed_arg,
)

import numpy as np
import pandas as pd
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

# ── Version ────────────────────────────────────────────────
BOT_VERSION = "2.2"   # trending: market-hours schedule (9:15+5min) · per-ticker price logging

# ── Constants ──────────────────────────────────────────────
ET             = ZoneInfo("America/New_York")
TRADE_LOG_FILE = Path("trade_log.csv")
PORT           = 8888

# ═══════════════════════════════════════════════════════════
#  STOCK INFO CACHE  (float + avg daily volume via yfinance)
# ═══════════════════════════════════════════════════════════



def get_stock_info(ticker: str) -> dict:
    """
    Return {float_m: float in millions, avg_vol: average daily volume}.
    Always fetches live data from yfinance and falls back to neutral defaults
    if yfinance is unavailable or the fetch fails.
    """
    if not _YF_AVAILABLE:
        return {"float_m": 0.0, "avg_vol": 0}

    ticker = ticker.strip().upper()
    if not ticker:
        return {"float_m": 0.0, "avg_vol": 0}

    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            info = yf.Ticker(ticker).info
        float_sh = info.get("floatShares") or info.get("sharesOutstanding") or 0
        avg_vol = (
            info.get("averageVolume10days")
            or info.get("averageDailyVolume10Day")
            or info.get("averageVolume")
            or 0
        )
        return {
            "float_m": round(float_sh / 1_000_000, 2) if float_sh else 0.0,
            "avg_vol": int(avg_vol),
        }
    except Exception:
        return {"float_m": 0.0, "avg_vol": 0}


def prefetch_stock_info(tickers: list, max_workers: int = 10):
    """
    Background-friendly batch fetch for a list of tickers.
    This does not cache results; it simply retrieves the latest data.
    """
    if not _YF_AVAILABLE or not tickers:
        return
    from concurrent.futures import ThreadPoolExecutor
    dlog.info(f"[INFO] Prefetching float/RVOL data for {len(tickers)} tickers …")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        list(ex.map(get_stock_info, tickers))
    dlog.info(f"[INFO] Stock info prefetch complete")


# ═══════════════════════════════════════════════════════════
#  SHARED STATE
# ═══════════════════════════════════════════════════════════

STATE = BotState()
STATE.config = load_config()   # overlay saved settings on top of defaults

# ═══════════════════════════════════════════════════════════
#  LOGGING BRIDGE
# ═══════════════════════════════════════════════════════════

class DashboardHandler(object):
    def info(self, msg):    STATE.add_log("INFO",    msg)
    def warning(self, msg): STATE.add_log("WARN",    msg)
    def error(self, msg):   STATE.add_log("ERROR",   msg)
    def debug(self, msg):   STATE.add_log("DEBUG",   msg)

dlog = DashboardHandler()
_trend.init(STATE, dlog)   # inject state+logger into trending module

# ═══════════════════════════════════════════════════════════
#  INDICATOR CALCULATIONS  (see signals.py)
# ═══════════════════════════════════════════════════════════

from signals import (
    ema, rsi, vwap_calc, williams_pr,
    compute_percent_r_exhaustion, compute_rmi,
    compute_obv_oscillator, compute_volume_trending_up,
    compute_macd, compute_signals, calc_rvol,
)

# ═══════════════════════════════════════════════════════════
#  ALPACA HELPERS
# ═══════════════════════════════════════════════════════════

import concurrent.futures as _cf_module
# Single 32-worker executor shared by:
#   • parallel bar fetch (submit+wait with 25s batch timeout)
#   • _timed() per-call wrappers for individual API calls
# 32 workers means the per-call wrappers can never starve the batch futures.
_FETCH_EXECUTOR = _cf_module.ThreadPoolExecutor(max_workers=32, thread_name_prefix="alpaca-fetch")

def _timed(fn, *args, timeout: float = 10, default=None, label: str = ""):
    """
    Run fn(*args) in _FETCH_EXECUTOR with a hard wall-clock timeout.
    Returns the result, or `default` if the call hangs / raises.
    The worker thread may live on after timeout, but the scan loop is unblocked.
    """
    fut = _FETCH_EXECUTOR.submit(fn, *args)
    try:
        return fut.result(timeout=timeout)
    except _cf_module.TimeoutError:
        dlog.warning(f"  [TIMEOUT] {label or fn.__name__} exceeded {timeout}s — skipped")
        return default
    except Exception as e:
        dlog.error(f"  [ERR] {label or fn.__name__}: {e}")
        return default

def get_live_price(data_client, ticker: str, cfg: dict = None) -> float | None:
    """Return latest close using Finnhub realtime state first, then Alpaca fallback."""
    cfg = cfg or STATE.config
    try:
        price = _fh.get_latest_price(ticker)
        if price is not None:
            return price
    except Exception:
        pass

    if data_client is None:
        return None
    return _api_get_latest_trade_price(data_client, ticker, cfg)


def fetch_bars(data_client, ticker: str, cfg: dict) -> pd.DataFrame | None:
    """Fetch historical bars using Finnhub if configured, else Alpaca."""
    cfg = cfg or STATE.config
    fh_api_key = cfg.get("finnhub_key") or os.getenv("FINNHUB_API_KEY", "")
    if fh_api_key:
        try:
            bars = _fh.fetch_bars(fh_api_key, ticker, cfg)
            if bars is not None:
                return bars
        except Exception as e:
            dlog.warning(f"Finnhub fetch_bars fallback failed for {ticker}: {e}")

    if data_client is None:
        return None
    return _api_fetch_bars(data_client, ticker, cfg)

def get_account_info(trading_client) -> dict:
    try:
        a = trading_client.get_account()
        return {
            "equity":      float(a.equity),
            "cash":        float(a.cash),
            "last_equity": float(a.last_equity) if hasattr(a,"last_equity") else float(a.equity),
            "buying_power":float(a.buying_power),
        }
    except Exception as e:
        dlog.error(f"get_account: {e}")
        return {}

def get_positions(trading_client, entry_prices: dict, high_water: dict) -> list:
    result = []
    try:
        for pos in trading_client.get_all_positions():
            ticker = pos.symbol
            ep     = entry_prices.get(ticker, float(pos.avg_entry_price))
            lp     = float(pos.current_price) if pos.current_price else ep
            qty    = int(float(pos.qty))
            pnl_d  = (lp - ep) * qty
            pnl_p  = (lp - ep) / ep * 100 if ep else 0
            result.append({
                "ticker":      ticker,
                "qty":         qty,
                "entry":       round(ep, 4),
                "live":        round(lp, 4),
                "pnl_dollars": round(pnl_d, 2),
                "pnl_pct":     round(pnl_p, 2),
                "high_water":  round(high_water.get(ticker, lp), 4),
                "market_value":round(float(pos.market_value), 2),
            })
    except Exception as e:
        dlog.error(f"get_positions: {e}")
    return result

def get_ask_price(data_client, ticker: str, cfg: dict = None) -> float | None:
    """Return the current ask price (bid as fallback) from Alpaca's latest quote."""
    try:
        from alpaca.data.requests import StockLatestQuoteRequest
        quotes = data_client.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=ticker, **_get_feed_arg(cfg))
        )
        q = quotes.get(ticker)
        if q:
            ask = float(q.ask_price or 0)
            if ask > 0:
                return ask
            bid = float(q.bid_price or 0)
            if bid > 0:
                return bid
        return None
    except Exception:
        return None


def get_bid_price(data_client, ticker: str, cfg: dict = None) -> float | None:
    """Return the current bid price (ask as fallback) from Alpaca's latest quote.
    Used for pre-market SELL limit orders — price at or slightly below bid
    gives the best fill probability during extended hours.
    """
    try:
        from alpaca.data.requests import StockLatestQuoteRequest
        quotes = data_client.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=ticker, **_get_feed_arg(cfg))
        )
        q = quotes.get(ticker)
        if q:
            bid = float(q.bid_price or 0)
            if bid > 0:
                return bid
            ask = float(q.ask_price or 0)
            if ask > 0:
                return ask
        return None
    except Exception:
        return None


def submit_order_smart(trading_client, ticker: str, qty: int, side_str: str,
                       limit_price: float | None = None, extended: bool = False):
    """
    Regular hours  → MarketOrderRequest  (price ignored by exchange)
    Pre/after hours → LimitOrderRequest with extended_hours=True
                      BUY  limit = ask + pre_market_limit_offset_pct  (caller sets)
                      SELL limit = bid - pre_market_limit_offset_pct  (caller sets)

    side_str    : "BUY" or "SELL"
    limit_price : required when extended=True; ignored when extended=False
    extended    : pass is_pre_market from the scan loop
    """
    from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
    from alpaca.trading.enums   import OrderSide, TimeInForce
    side = OrderSide.BUY if side_str == "BUY" else OrderSide.SELL
    try:
        if extended:
            if not limit_price:
                raise ValueError(f"limit_price required for extended-hours order ({ticker})")
            return trading_client.submit_order(LimitOrderRequest(
                symbol=ticker, qty=qty, side=side,
                time_in_force=TimeInForce.DAY,
                limit_price=round(limit_price, 4),
                extended_hours=True,
            ))
        else:
            return trading_client.submit_order(MarketOrderRequest(
                symbol=ticker, qty=qty, side=side,
                time_in_force=TimeInForce.DAY,
            ))
    except Exception as _oe:
        _om = str(_oe)
        if "pattern day trading" in _om.lower() or "40310100" in _om:
            raise RuntimeError(
                f"PDT protection: {ticker} {side_str} blocked. "
                "Cash account users: your paper account defaults to margin+PDT — "
                "fix in Alpaca dashboard → Paper account → Settings → "
                "uncheck 'Enable PDT Protection'. PDT never applies to your live cash account."
            ) from _oe
        raise


def close_position(trading_client, ticker: str) -> bool:
    return _api_close_position(trading_client, ticker)

def load_today_trades() -> list:
    today = datetime.now(ET).strftime("%Y-%m-%d")
    rows  = []
    if TRADE_LOG_FILE.exists():
        try:
            with open(TRADE_LOG_FILE, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("date") == today:
                        rows.append(row)
        except Exception:
            pass
    return rows

def load_all_trades(days: int = 90) -> list:
    """Read all trades from the CSV going back `days` calendar days."""
    rows = []
    if not TRADE_LOG_FILE.exists():
        return rows
    cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
    try:
        with open(TRADE_LOG_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("date", "") >= cutoff:
                    rows.append(row)
    except Exception:
        pass
    return rows

def now_et() -> datetime:
    return datetime.now(ET)

# ═══════════════════════════════════════════════════════════
#  FAST-SCAN THREAD (1-second pre-check polling)
# ═══════════════════════════════════════════════════════════

def fast_scan_thread(state: BotState):
    """
    Polls tickers in STATE.watching every 1 second.

    A ticker enters the watch set (via the main scan loop) when both its
    fast (%R 21) and slow (%R 112) lines exceed the precheck_threshold (default -40)
    and are both trending upward.  This thread runs the full signal stack on those
    tickers at 1-second resolution so the buy signal isn't missed between
    the main loop's longer scan interval.

    A ticker is removed from the watch set when:
      • A BUY signal fires (logged, main loop will execute the order)
      • Either %R line falls back below the precheck_threshold (conditions failed)
      • The ticker is no longer available (data fetch failure x 3)
    """
    fail_counts: dict = {}   # consecutive data-fetch failures per ticker

    while not state.stop_event.is_set():
        with state.lock:
            watch_copy  = dict(state.watching)
            cfg         = dict(state.config)
            data_client = state.data_client

        if not watch_copy or data_client is None:
            state.stop_event.wait(timeout=1)
            continue

        # Use a smaller bar count for fast-scan — just enough for the indicators we actually use.
        # When use_rmi=False, SMA(200) is not needed — drop from 260 bars to ~60.
        fast_cfg = dict(cfg)
        if cfg.get("use_rmi", True):
            fast_cfg["bar_count"] = max(260, cfg.get("rmi_ma_slow", 200) + 60)
            fast_min = max(cfg.get("ema_long", 21) + 5, cfg.get("rmi_ma_slow", 200) + 10)
        else:
            # Only need MACD (26+9=35) + vol MAs (20) + EMA warm-up — 60 bars is plenty
            fast_cfg["bar_count"] = max(60, cfg.get("macd_slow", 26) + cfg.get("macd_signal", 9) + 30)
            fast_min = cfg.get("ema_long", 21) + 5
        precheck_thr = cfg.get("precheck_threshold", -40)

        # ── Fetch bars for all watched tickers in a single Alpaca batch request.
        fast_results: dict[str, dict] = {}
        batch_bars = _api_fetch_bars_batch(data_client, list(watch_copy), fast_cfg) if data_client else {}

        for ticker in watch_copy:
            df = batch_bars.get(ticker)
            if df is None or len(df) < fast_min:
                df = fetch_bars(data_client, ticker, fast_cfg) if data_client else None
            if df is None or len(df) < fast_min:
                fast_results[ticker] = None
                continue
            try:
                fast_results[ticker] = compute_signals(df, fast_cfg)
            except Exception:
                fast_results[ticker] = None

        for ticker in list(watch_copy.keys()):
            if state.stop_event.is_set():
                break
            df = fast_results.get(ticker)
            if df is None:
                fail_counts[ticker] = fail_counts.get(ticker, 0) + 1
                if fail_counts[ticker] >= 3:
                    with state.lock:
                        state.watching.pop(ticker, None)
                    dlog.debug(f"  [FAST] {ticker} removed — repeated data failures")
                continue

            fail_counts[ticker] = 0
            latest   = df.iloc[-1]
            signal   = latest["signal"]
            rte_fast = float(latest.get("rte_fast", -100))
            rte_slow = float(latest.get("rte_slow", -100))
            streak   = int(latest.get("rte_boxes_streak", 0))

            if signal == "BUY":
                lp = float(latest["close"])
                dlog.info(f"  [FAST] ★ {ticker} BUY signal @ ${lp:.3f} — "
                          f"streak={streak} f={rte_fast:.0f} s={rte_slow:.0f}")
                _conds = {
                    "reversal": bool(latest.get("rte_reversal", False)),
                    "streak_ok": True, "streak": streak,
                    "rmi_ok": bool(latest.get("rmi_signal", False)),
                    "vol_ok": bool(latest.get("vol_trend_up", False)) or bool(latest.get("vol_surge", False)),
                    "macd_ok": float(latest.get("macd_line", 0) or 0) > float(latest.get("macd_signal_line", 0) or 0),
                    "rte_fast": rte_fast, "rte_slow": rte_slow,
                }
                with state.lock:
                    state.watching.pop(ticker, None)
                state.add_ondeck_event("exit_buy", ticker, f"★ BUY signal @ ${lp:.3f}", _conds)
            elif streak == 0:
                # Both lines dropped below failure threshold — setup abandoned
                dlog.info(f"  [FAST] {ticker} ← Off Deck "
                          f"(f={rte_fast:.0f} s={rte_slow:.0f} streak reset)")
                with state.lock:
                    state.watching.pop(ticker, None)
                state.add_ondeck_event("exit_fail", ticker,
                    f"Setup failed — %R retreated (f={rte_fast:.0f} s={rte_slow:.0f})",
                    {"rte_fast": rte_fast, "rte_slow": rte_slow})
            else:
                rmi_v = float(latest.get("rmi", 50) or 50)
                dlog.debug(f"  [FAST] {ticker} streak={streak} f={rte_fast:.0f} s={rte_slow:.0f} "
                           f"rmi={rmi_v:.1f} → {signal}")
                # Resolve per-ticker min_boxes (respects ticker_overrides)
                _overrides   = cfg.get("ticker_overrides", {}).get(ticker, {})
                _min_boxes   = _overrides.get("rte_min_boxes", cfg.get("rte_min_boxes", 2))
                _min_support = cfg.get("rte_min_supporting", 1)
                # Individual signal conditions
                _reversal = bool(latest.get("rte_reversal", False))
                _rmi_ok   = bool(latest.get("rmi_signal",   False))
                _vol_ok   = bool(latest.get("vol_trend_up", False)) or bool(latest.get("vol_surge", False))
                _macd_ok  = float(latest.get("macd_line", 0) or 0) > float(latest.get("macd_signal_line", 0) or 0)
                # Refresh stored values so the On Deck card stays current
                with state.lock:
                    if ticker in state.watching:
                        state.watching[ticker].update({
                            "rte_fast":    rte_fast,
                            "rte_slow":    rte_slow,
                            "streak":      streak,
                            "reversal":    _reversal,
                            "streak_ok":   streak >= _min_boxes,
                            "min_boxes":   _min_boxes,
                            "rmi_ok":      _rmi_ok,
                            "rmi_val":     rmi_v,
                            "vol_ok":      _vol_ok,
                            "macd_ok":     _macd_ok,
                            "support":     int(_rmi_ok) + int(_vol_ok) + int(_macd_ok),
                            "min_support": _min_support,
                        })

        # ── Fast-watch: open position exit monitoring ──────────────────────
        # Poll live prices for every open position and fire exits (stop/TP/trail/partial)
        # if triggered — without waiting for the next main-loop scan cycle.
        # Coordinates with the main loop via state.fast_closing:
        #   • fast_scan adds the ticker to fast_closing before submitting the order
        #   • main loop skips exit logic for tickers in fast_closing
        #   • fast_closing is cleared here once the order round is done
        with state.lock:
            _ep_copy  = dict(state.entry_prices)
            _hw_copy  = dict(state.high_water)
            _fc_copy  = set(state.fast_closing)
            _pe_copy  = set(state.partial_exited)
            _trading  = state.trading_client
            _dclient  = state.data_client

        if _ep_copy and _trading and _dclient:
            # Fetch live prices once for all open positions.
            _live_prices = {}
            if _dclient:
                _live_prices = _api_get_latest_trade_prices(_dclient, list(_ep_copy.keys()), cfg)
            for _t in list(_ep_copy.keys()):
                price = _fh.get_latest_price(_t)
                if price is not None:
                    _live_prices[_t] = price

            _newly_closing = set()
            for ticker, ep in list(_ep_copy.items()):
                if state.stop_event.is_set():
                    break
                if ticker in _fc_copy:
                    continue  # already being closed by an earlier fast-watch cycle
                lp = _live_prices.get(ticker)
                if not lp:
                    continue
                hw  = _hw_copy.get(ticker, lp)
                chg = (lp - ep) / ep if ep else 0

                # Update high-water mark
                if lp > hw:
                    hw = lp
                    with state.lock:
                        state.high_water[ticker] = lp

                exit_reason = None

                # Hard stop
                if chg <= -cfg.get("stop_loss_pct", 0.04):
                    exit_reason = ("STOP", f"stop {chg:.2%}", "full")

                # Take profit
                elif chg >= cfg.get("take_profit_pct", 0.12):
                    exit_reason = ("TP", f"take-profit {chg:.2%}", "full")

                # Trailing stop
                elif (hw - ep) / ep >= cfg.get("trail_activation_pct", 0.05):
                    trail_pct = cfg.get("trail_stop_pct", 0.02)
                    trigger   = max(hw * (1 - trail_pct), ep)
                    if lp <= trigger:
                        exit_reason = ("TRAIL", f"trail HW=${hw:.3f} trig=${trigger:.3f}", "full")

                # Partial exit
                elif (cfg.get("partial_exit_enabled")
                        and ticker not in _pe_copy
                        and chg >= cfg.get("partial_exit_pct", 0.06)):
                    exit_reason = ("PARTIAL", f"partial +{chg:.2%}", "partial")

                if not exit_reason:
                    continue

                tag, reason_str, exit_type = exit_reason

                # Look up current qty from Alpaca
                try:
                    _qty_resp = {p.symbol: int(float(p.qty))
                                 for p in _trading.get_all_positions()}
                    qty_held = _qty_resp.get(ticker, 0)
                except Exception:
                    qty_held = 0

                if qty_held <= 0:
                    # Position already gone — clean up state
                    with state.lock:
                        state.entry_prices.pop(ticker, None)
                        state.high_water.pop(ticker, None)
                    continue

                if exit_type == "partial":
                    sell_qty = max(1, int(qty_held * cfg.get("partial_exit_qty_pct", 0.50)))
                    if sell_qty >= qty_held:
                        sell_qty = max(1, qty_held - 1)  # keep at least 1 share
                    if sell_qty <= 0:
                        continue
                else:
                    sell_qty = qty_held

                # Mark as fast-closing BEFORE submitting so main loop skips it
                with state.lock:
                    state.fast_closing.add(ticker)
                _newly_closing.add(ticker)

                dlog.info(f"  [FAST-EXIT] {ticker} {tag} @ ${lp:.3f} {reason_str}  "
                          f"qty={sell_qty}  P&L≈${(lp-ep)*sell_qty:+.2f}")
                try:
                    submit_order_smart(_trading, ticker, sell_qty, "SELL")
                    # Update state so main loop doesn't double-fire
                    with state.lock:
                        if exit_type == "partial":
                            state.partial_exited.add(ticker)
                        else:
                            state.entry_prices.pop(ticker, None)
                            state.high_water.pop(ticker, None)
                    # Add event to On Deck feed for visibility
                    state.add_ondeck_event(
                        "exit_position", ticker,
                        f"{tag} @ ${lp:.3f} {reason_str}",
                        {"chg": round(chg * 100, 2), "qty": sell_qty,
                         "tag": tag, "ep": ep, "lp": lp}
                    )
                except Exception as ex:
                    dlog.error(f"  [FAST-EXIT] {ticker} order failed: {ex}")
                    with state.lock:
                        state.fast_closing.discard(ticker)
                    _newly_closing.discard(ticker)

            # After this poll round, release fast_closing so the main loop can
            # see the position is gone and clean up its own state.
            # We keep the ticker in fast_closing for one extra main-loop cycle
            # by NOT clearing it here — it clears on next fast_scan iteration.
            # (The main loop checks fast_closing and skips, then the clear below
            #  removes it, so next main-loop iteration handles the cleanup.)
            if _newly_closing:
                # Brief sleep to let the main loop see the order, then release
                state.stop_event.wait(timeout=2)
                with state.lock:
                    for _t in _newly_closing:
                        state.fast_closing.discard(_t)

        state.stop_event.wait(timeout=1)

# ═══════════════════════════════════════════════════════════
#  BOT THREAD
# ═══════════════════════════════════════════════════════════

def bot_thread(state: BotState):
    cfg = state.config
    mode = 'PAPER' if cfg.get('paper') else 'LIVE'
    dlog.info("=" * 50)
    dlog.info(f"  Bot starting — {mode} mode")
    dlog.info(f"  API key: {cfg.get('api_key','')[:8]}…   tickers: {len(cfg.get('tickers',[]))}")
    dlog.info("=" * 50)

    fh_api_key = cfg.get("finnhub_key") or os.getenv("FINNHUB_API_KEY", "")
    if fh_api_key:
        fh_thread = _fh.start_finnhub_stream(fh_api_key, cfg.get("tickers", []))
        if fh_thread:
            dlog.info(f"  Finnhub stream started for {len(cfg.get('tickers', []))} tickers")
        else:
            dlog.warning("  Finnhub stream disabled or failed to start")
    else:
        dlog.warning("  No Finnhub API key configured; realtime stream will be disabled")

    dlog.info(f"  Connecting to Alpaca ({mode})…")

    try:
        trading, data = connect_alpaca(cfg)
        with state.lock:
            state.trading_client = trading
            state.data_client    = data
            state.error          = None
        acc = get_account_info(trading)
        with state.lock:
            state.account = acc
        dlog.info(f"  Connected | Equity: ${acc.get('equity',0):,.2f}  Cash: ${acc.get('cash',0):,.2f}")
        # Kick off float/RVOL prefetch in background so first scan isn't slow
        # Include both watchlist and any already-fetched trending tickers
        with state.lock:
            all_tickers = list(dict.fromkeys(cfg.get("tickers", []) + list(state.trending_tickers)))
        t_pre = threading.Thread(
            target=prefetch_stock_info,
            args=(all_tickers,),
            daemon=True,
        )
        t_pre.start()
        # Start the 1-second fast-scan thread for pre-check tickers
        t_fast = threading.Thread(target=fast_scan_thread, args=(state,), daemon=True)
        t_fast.start()
    except Exception as e:
        with state.lock:
            state.error   = str(e)
            state.running = False
        dlog.error(f"  Connection failed: {e}")
        return

    entry_prices   = {}
    high_water     = {}
    pyramided      = set()
    partial_exited = set()   # tickers that already had a partial exit today
    daily_buys     = 0
    last_date      = None
    eod_done       = None
    day_open_equity  = None   # equity at start of trading day — for daily loss limit
    daily_loss_halt  = False  # True = daily loss limit hit, no new buys today

    def _check_buys_ok(now, daily_b, n_pos):
        nb_h,  nb_m  = cfg.get("no_new_buys_after",  [15, 30])
        nbb_h, nbb_m = cfg.get("no_new_buys_before", [9,  30])  # default = open
        return (
            (now.hour, now.minute) >= (nbb_h, nbb_m)           # after open window
            and (now.hour, now.minute) <  (nb_h,  nb_m)        # before close window
            and daily_b < cfg.get("max_daily_buys", 10)
            and n_pos   < cfg.get("max_positions",  10)
        )

    init_done = False
    _TRADE_COLS = [
        "date","time","ticker","action","qty","price",
        "position_value","entry_price","pnl_dollars","pnl_pct","reason",
        # Signal context (populated on BUY; empty on exits)
        "streak","float_m","rvol","rsi2","rte_fast","rte_slow","conviction",
    ]

    if not TRADE_LOG_FILE.exists():
        with open(TRADE_LOG_FILE, "w", newline="") as f:
            csv.writer(f).writerow(_TRADE_COLS)

    def log_trade(ticker, action, qty, price, entry_price=None, reason="SIGNAL", **ctx):
        now_t = datetime.now()
        pnl_d = pnl_p = ""
        if entry_price and action in ("SELL","STOP","TP","EOD","TRAIL","PYRAMID","PARTIAL","EXHAUSTION"):
            pnl_d = round((price - entry_price) * qty, 2)
            pnl_p = round((price - entry_price) / entry_price * 100, 2)
        row = [
            now_t.strftime("%Y-%m-%d"), now_t.strftime("%H:%M:%S"),
            ticker, action, qty, round(price, 4), round(price * qty, 2),
            round(entry_price, 4) if entry_price else "", pnl_d, pnl_p, reason,
            # Signal context fields (default empty for exit rows)
            ctx.get("streak",     ""),
            ctx.get("float_m",    ""),
            ctx.get("rvol",       ""),
            ctx.get("rsi2",       ""),
            ctx.get("rte_fast",   ""),
            ctx.get("rte_slow",   ""),
            ctx.get("conviction", ""),
        ]
        with open(TRADE_LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
        with state.lock:
            state.today_trades.append(dict(zip(_TRADE_COLS, [str(v) for v in row])))

    while not state.stop_event.is_set():
        now   = now_et()
        today = now.date()

        if last_date != today:
            if last_date is not None:
                dlog.info("  [NEW DAY] Resetting counters")
            daily_buys       = 0
            pyramided        = set()
            partial_exited   = set()
            last_date        = today
            day_open_equity  = None   # will be set on first scan of the day
            daily_loss_halt  = False

        eod_h, eod_m = cfg.get("eod_liquidate_at", [15,50])
        # Only fire EOD liquidation if we were actually in session today.
        # Guards against cold-start after market close (eod_done=None but market
        # is already shut — no positions to sell, order would fail anyway).
        _session_was_open_today = eod_done == today or state.last_scan is not None
        if (now.hour, now.minute) >= (eod_h, eod_m) and eod_done != today:
            eod_done = today   # mark done first so restart loops don't repeat
            if _session_was_open_today:
                dlog.info("  [EOD] Liquidating all positions …")
                _eod_positions = _timed(trading.get_all_positions, timeout=10, default=[], label="eod_get_positions") or []
                for pos in _eod_positions:
                    t   = pos.symbol
                    qty = int(float(pos.qty))
                    lp  = _timed(get_live_price, data, t, timeout=8, default=0.0, label=f"eod_live_price:{t}") or 0.0
                    ep  = entry_prices.get(t, lp)
                    try:
                        from alpaca.trading.requests import MarketOrderRequest
                        from alpaca.trading.enums    import OrderSide, TimeInForce
                        trading.submit_order(MarketOrderRequest(
                            symbol=t, qty=qty,
                            side=OrderSide.SELL,
                            time_in_force=TimeInForce.DAY,
                        ))
                        log_trade(t, "EOD", qty, lp, ep, "EOD")
                        dlog.info(f"  [EOD] Sold {qty} x {t}")
                    except Exception as ex:
                        _emsg = str(ex)
                        if "pattern day trading" in _emsg.lower() or "40310100" in _emsg:
                            dlog.warning(f"  [EOD] {t} PDT protection — cannot day-trade (account < $25k). "
                                         f"Position will be held overnight. Disable PDT in Alpaca paper settings to allow.")
                        else:
                            dlog.error(f"  [EOD] {t} order failed: {ex}")
            else:
                dlog.info("  [EOD] Bot started after market close — skipping liquidation (no session today)")
            entry_prices.clear(); high_water.clear(); pyramided.clear(); partial_exited.clear()
            with state.lock:
                state.positions    = []
                state.entry_prices = {}
                state.high_water   = {}
            secs = max(60, int((now.replace(hour=9,minute=29,second=0,microsecond=0) + timedelta(days=1) - now).total_seconds()))
            dlog.info(f"  [EOD] Sleeping {secs//3600}h {(secs%3600)//60}m until next open")
            state.stop_event.wait(timeout=min(secs, 3600))
            continue

        regular_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
        if cfg.get("pre_market_enabled"):
            pm_h, pm_m   = cfg.get("pre_market_start", [4, 0])
            session_open = now.replace(hour=pm_h, minute=pm_m, second=0, microsecond=0)
        else:
            session_open = regular_open
        is_pre_market = now < regular_open   # True during 4:00–9:30 window
        if not (session_open <= now < market_close):
            dlog.info(f"  Market closed — sleeping 5 min")
            state.stop_event.wait(timeout=300)
            continue

        try:
            acc  = _timed(get_account_info, trading, timeout=10, default={}, label="get_account_info")
            pval = acc.get("equity", 0)
            with state.lock:
                state.account = acc

            # ── Capture day-open equity on first scan ──────────────
            if day_open_equity is None and pval > 0:
                day_open_equity = pval
                dlog.info(f"  [DAY OPEN] Equity baseline: ${day_open_equity:,.2f}")

            # ── Daily max loss halt ────────────────────────────────
            max_loss_pct = cfg.get("max_daily_loss_pct", 0.05)
            if day_open_equity and pval > 0 and not daily_loss_halt:
                day_loss = (pval - day_open_equity) / day_open_equity
                if day_loss <= -max_loss_pct:
                    daily_loss_halt = True
                    dlog.warning(f"  [HALT] Daily loss limit hit: {day_loss:.2%} (max -{max_loss_pct:.0%}) — no new buys today")
            if daily_loss_halt:
                day_loss = (pval - day_open_equity) / day_open_equity if day_open_equity else 0
                dlog.info(f"  [HALT] Trading halted for today — day P&L: {day_loss:.2%}  (restart tomorrow)")

            _raw_positions = _timed(trading.get_all_positions, timeout=10, default=[], label="get_all_positions")
            open_pos  = {p.symbol: int(float(p.qty)) for p in (_raw_positions or [])}
            n_pos     = len(open_pos)
            buys_ok   = _check_buys_ok(now, daily_buys, n_pos)
            # Sync partial_exited set from fast-watch thread so main loop
            # doesn't repeat a partial exit that fast-watch already fired
            with state.lock:
                partial_exited |= state.partial_exited
            scan_ts   = now.strftime("%H:%M:%S ET")
            # Merge user watchlist + Yahoo/Finviz trending (deduped, watchlist first)
            watchlist = cfg.get("tickers", [])
            with state.lock:
                trending = list(state.trending_tickers)
            seen = set()
            scan_tickers = []
            for t in watchlist + trending:
                if t not in seen:
                    seen.add(t)
                    scan_tickers.append(t)
            dlog.info(f"--- Scan {scan_ts}  Equity=${pval:,.0f}  Pos={n_pos}/10  Buys={daily_buys}/10  Tickers={len(scan_tickers)}({len(watchlist)}wl+{len([t for t in trending if t not in set(watchlist)])}tr) ---")

            trending_set  = set(trending)
            watchlist_set = set(watchlist)

            # ── Fetch all bars in parallel; hard 25s wall-clock timeout ─────
            # Using submit+wait instead of map so a single slow ticker can't
            # stall the scan indefinitely (zombie threads don't block future scans).
            _futures = {_FETCH_EXECUTOR.submit(fetch_bars, data, t, cfg): t
                        for t in scan_tickers}
            _done, _slow = _cf_module.wait(_futures.keys(), timeout=25)
            bar_results = {}
            for _fut in _done:
                _t = _futures[_fut]
                try:    bar_results[_t] = _fut.result()
                except: bar_results[_t] = None
            for _fut in _slow:
                _t = _futures[_fut]
                bar_results[_t] = None
                dlog.warning(f"  {_t:<6} bar fetch still running after 25s — skipped")

            # Minimum bars: EMA warm-up always needed; SMA(200) only when use_rmi=True
            _ema_min = cfg.get("ema_long", 21) + 5
            _rmi_min = cfg.get("rmi_ma_slow", 200) + 10 if cfg.get("use_rmi", True) else 0
            min_bars = max(_ema_min, _rmi_min)

            # ── Compute signals in parallel for all valid tickers ────────────
            # Pandas releases the GIL for numpy ops, so threading gives real
            # throughput gains — all compute_signals calls run concurrently.
            def _sig_job(args):
                _t, _df, _cfg = args
                try:    return _t, compute_signals(_df, _cfg)
                except Exception as _ex:
                    dlog.debug(f"  [SIG] {_t} compute error: {_ex}")
                    return _t, None

            _sig_inputs = [
                (t, bar_results[t], cfg)
                for t in scan_tickers
                if bar_results.get(t) is not None and len(bar_results[t]) >= min_bars
            ]
            _sig_futs = {_FETCH_EXECUTOR.submit(_sig_job, inp): inp[0]
                         for inp in _sig_inputs}
            _sig_done, _sig_slow = _cf_module.wait(_sig_futs.keys(), timeout=20)
            sig_results: dict = {}
            for _f in _sig_done:
                _t = _sig_futs[_f]
                try:
                    _, _df_sig = _f.result()
                    sig_results[_t] = _df_sig
                except:
                    sig_results[_t] = None
            for _f in _sig_slow:
                _t = _sig_futs[_f]
                sig_results[_t] = None
                dlog.warning(f"  {_t:<6} signal compute timed out — skipped")

            for ticker in scan_tickers:
                if state.stop_event.is_set():
                    break

                is_trending = ticker in trending_set and ticker not in watchlist_set

                # ── Float filter (watchlist only — trending stocks bypass) ───
                si = get_stock_info(ticker)
                fm = si.get("float_m", 0.0)
                if not is_trending and cfg.get("use_float_filter") and cfg.get("max_float_million", 0) > 0:
                    if fm > 0 and fm > cfg["max_float_million"]:
                        dlog.info(f"  {ticker:<6} SKIP — float {fm:.0f}M > max {cfg['max_float_million']}M")
                        continue

                df = bar_results.get(ticker)
                if df is None:
                    dlog.info(f"  {ticker:<6} SKIP — no bar data returned")
                    continue
                if len(df) < min_bars:
                    dlog.info(f"  {ticker:<6} SKIP — only {len(df)} bars (need {min_bars})")
                    continue

                # ── Volume hard gate — no volume, no trade ────────────────
                # During pre-market the 20-bar rolling average is dominated by
                # yesterday's regular-hours bars (10-100x the pre-market bar size),
                # so we use a separate lower multiplier to avoid blocking every ticker.
                vol_window  = cfg.get("volume_long_ma", 20)
                if is_pre_market:
                    vol_mult = cfg.get("pre_market_volume_surge_mult", 0.3)
                else:
                    vol_mult = cfg.get("volume_surge_mult", 1.5)
                avg_vol_bar = df["volume"].rolling(vol_window).mean().iloc[-1]
                cur_vol     = df["volume"].iloc[-1]
                if avg_vol_bar > 0 and cur_vol < avg_vol_bar * vol_mult:
                    session_tag = "PM" if is_pre_market else "REG"
                    dlog.info(f"  {ticker:<6} SKIP — no volume: {cur_vol:.0f} < {avg_vol_bar * vol_mult:.0f} ({vol_mult}x avg) [{session_tag}]")
                    continue

                # ── Daily RVOL filter ─────────────────────────
                rvol = 0.0
                avg_vol = 0
                if cfg.get("use_rvol") and cfg.get("min_rvol", 0) > 0:
                    avg_vol = si.get("avg_vol", 0)   # yfinance fallback, may be 0
                    rvol = calc_rvol(df, avg_vol)
                    if rvol.iloc[-1] > 0 and rvol.iloc[-1] < cfg["min_rvol"]:
                        dlog.info(f"  {ticker:<6} SKIP — rvol {rvol.iloc[-1]:.2f} < min {cfg['min_rvol']}")
                        continue

                df = sig_results.get(ticker)
                if df is None:
                    dlog.info(f"  {ticker:<6} SKIP — signal compute failed")
                    continue
                latest = df.iloc[-1]
                signal = latest["signal"]
                price  = float(latest["close"])
                rsi_v  = float(latest["rsi"])
                ema_s  = float(latest["ema_short"])
                ema_l  = float(latest["ema_long"])
                vol_s  = bool(latest.get("vol_surge", False))
                vwap_v = float(latest.get("vwap", 0) or 0)
                lp     = _timed(get_live_price, data, ticker, timeout=8, default=None, label=f"live_price:{ticker}") or price

                # ── On Deck / fast-scan entry ───────────────────────────────
                # streak ≥ 1 (first box fired) → enter On Deck watch set.
                # streak = 0 (setup abandoned, both lines below threshold) → remove.
                # Only for exhaustion strategy on tickers not already held.
                if cfg.get("strategy", "exhaustion") == "exhaustion" and not (ticker in open_pos):
                    _rte_fast_now  = float(latest.get("rte_fast", -100))
                    _rte_slow_now  = float(latest.get("rte_slow", -100))
                    _streak_now    = int(latest.get("rte_boxes_streak", 0))
                    _in_watch      = ticker in state.watching
                    if _streak_now >= 1 and not _in_watch:
                        _si       = get_stock_info(ticker)
                        _float_m  = _si.get("float_m", 0.0)
                        _micro_thr = cfg.get("micro_float_threshold", 10)
                        _micro_tag = " ⚡MICRO" if 0 < _float_m <= _micro_thr else ""
                        with state.lock:
                            state.watching[ticker] = {
                                "rte_fast": _rte_fast_now,
                                "rte_slow": _rte_slow_now,
                                "streak":   _streak_now,
                                "float_m":  _float_m,
                            }
                        dlog.info(f"  [ON DECK] {ticker} → streak={_streak_now} f={_rte_fast_now:.0f} s={_rte_slow_now:.0f} float={_float_m:.1f}M{_micro_tag}")
                        _enter_conds = {
                            "rte_fast":  _rte_fast_now,
                            "rte_slow":  _rte_slow_now,
                            "streak":    _streak_now,
                            "float_m":   _float_m,
                            "micro":     0 < _float_m <= _micro_thr,
                        }
                        state.add_ondeck_event(
                            "enter", ticker,
                            f"streak={_streak_now} f={_rte_fast_now:.0f} s={_rte_slow_now:.0f}",
                            _enter_conds
                        )
                    elif _in_watch and _streak_now == 0:
                        with state.lock:
                            state.watching.pop(ticker, None)
                        dlog.info(f"  [ON DECK] {ticker} ← removed (streak reset — setup failed)")

                # ── Pre-market order prices ─────────────────────────────
                # BUY  → ask + offset  (fetch below, at signal time)
                # SELL → bid - offset  (fetched now, reused for all exit types)
                _pm_offset = cfg.get("pre_market_limit_offset_pct", 0.002)
                if is_pre_market:
                    bid_pm  = _timed(get_bid_price, data, ticker, timeout=8, default=None, label=f"bid_price:{ticker}")
                    sell_px = bid_pm * (1 - _pm_offset) if bid_pm else lp
                else:
                    bid_pm  = None
                    sell_px = lp  # market order — price is ignored by exchange

                # ── Price sanity check — skip split-adjusted / bad data ──
                if price > 0 and abs(lp - price) / price > 0.20:
                    dlog.warning(f"  {ticker:<6} SKIP — price mismatch: live=${lp:.3f} vs bar=${price:.4f} (likely reverse split — remove from watchlist)")
                    continue

                # ── Per-ticker signal status (always logged) ───────
                strat = cfg.get("strategy", "exhaustion")
                if strat == "exhaustion":
                    rte_ext       = bool(latest.get("rte_extreme",        False))
                    rte_rev       = bool(latest.get("rte_reversal",       False))
                    rte_boxes_streak = int(latest.get("rte_boxes_streak",   0))
                    rte_boxes_all    = int(latest.get("rte_boxes_completed", 0))
                    rte_fast_v    = float(latest.get("rte_fast",          -100))
                    rte_slow_v    = float(latest.get("rte_slow",          -100))
                    rmi_v         = float(latest.get("rmi",               50))
                    rmi_sig       = bool(latest.get("rmi_signal",         False))
                    vol_up        = bool(latest.get("vol_trend_up",       False))
                    macd_bull     = bool(latest.get("macd_bull",          False))
                    macd_bear     = bool(latest.get("macd_bear",          False))
                    rmi_oversold  = cfg.get("rmi_oversold", 10)
                    min_boxes     = cfg.get("rte_min_boxes", 3)
                    def _t(v): return "✓" if v else "✗"
                    dlog.info(
                        f"  {ticker:<6} ${lp:.3f}  "
                        f"rte={_t(rte_ext)}(f={rte_fast_v:.0f} s={rte_slow_v:.0f} rev={_t(rte_rev)} streak={rte_boxes_streak}/{min_boxes},{rte_boxes_all}tot)  "
                        f"rsi2={rmi_v:.1f}/{rmi_oversold}{_t(rmi_sig)}  "
                        f"vol↑={_t(vol_up)}  macd↑={_t(macd_bull)}  "
                        f"→ {signal if signal else 'HOLD'}"
                    )
                elif strat == "ema_crossover":
                    cross_up = bool(latest.get("cross_up", False))
                    dlog.info(
                        f"  {ticker:<6} ${lp:.3f}  "
                        f"rsi={rsi_v:.1f}  ema_s={ema_s:.3f}/ema_l={ema_l:.3f}  "
                        f"cross_up={'✓' if cross_up else '✗'}  vol={'✓' if vol_s else '✗'}  "
                        f"→ {signal if signal else 'HOLD'}"
                    )
                elif strat == "macd":
                    macd_bull = bool(latest.get("macd_bull", False))
                    macd_bear = bool(latest.get("macd_bear", False))
                    dlog.info(
                        f"  {ticker:<6} ${lp:.3f}  "
                        f"macd↑={'✓' if macd_bull else '✗'}  macd↓={'✓' if macd_bear else '✗'}  "
                        f"vol={'✓' if vol_s else '✗'}  "
                        f"→ {signal if signal else 'HOLD'}"
                    )
                # ── Verbose debug (debug_signals=true adds extra detail) ─
                if cfg.get("debug_signals") and signal:
                    dlog.info(f"  [DEBUG] {ticker} bars={len(df)}  close={price:.4f}  rsi={rsi_v:.2f}")

                in_pos   = ticker in open_pos
                qty_held = open_pos.get(ticker, 0)

                if in_pos:
                    hw = high_water.get(ticker, lp)
                    if lp > hw:
                        high_water[ticker] = lp

                if in_pos and ticker in entry_prices:
                    # Skip exit logic if fast-watch already submitted a close order
                    with state.lock:
                        _is_fast_closing = ticker in state.fast_closing
                    if _is_fast_closing:
                        dlog.debug(f"  {ticker:<6} skip exits — fast-watch order in flight")
                        continue

                    ep  = entry_prices[ticker]
                    chg = (lp - ep) / ep if ep else 0
                    hw  = high_water.get(ticker, lp)

                    # Exhaustion re-entry exit (in-position check only)
                    # If price goes BACK into the red extreme zone after we bought
                    # the reversal, the stock recovered to overbought — exit at the
                    # top rather than risk giving it all back.
                    # Checked here (not in compute_signals) so it only fires when
                    # we actually hold the position — not for every overbought stock.
                    latest_row = df.iloc[-1]
                    if (cfg.get("use_rte_exhaustion", True)
                            and bool(latest_row.get("rte_extreme", False))):
                        _sell_tag = f"LIMIT@${sell_px:.4f}(bid${bid_pm:.4f})" if is_pre_market else f"MKT@${sell_px:.3f}"
                        dlog.info(f"  [EXHST] {ticker} back in extreme zone {chg:.2%}  {_sell_tag}  P&L: ${(lp-ep)*qty_held:+.2f}")
                        try:
                            submit_order_smart(trading, ticker, qty_held, "SELL",
                                               limit_price=sell_px, extended=is_pre_market)
                            log_trade(ticker, "EXHAUSTION", qty_held, lp, ep, "EXHAUSTION-REENTRY")
                            del entry_prices[ticker]; high_water.pop(ticker,None)
                            open_pos.pop(ticker,None); n_pos=max(0,n_pos-1)
                            buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                        except Exception as ex:
                            dlog.error(f"  EXHST order failed {ticker}: {ex}")
                        continue

                    # Hard stop
                    if chg <= -cfg.get("stop_loss_pct", 0.04):
                        _sell_tag = f"LIMIT@${sell_px:.4f}(bid${bid_pm:.4f})" if is_pre_market else f"MKT@${sell_px:.3f}"
                        dlog.info(f"  [STOP] {ticker} {chg:.2%}  {_sell_tag}  P&L: ${(lp-ep)*qty_held:+.2f}")
                        try:
                            submit_order_smart(trading, ticker, qty_held, "SELL",
                                               limit_price=sell_px, extended=is_pre_market)
                            log_trade(ticker, "STOP", qty_held, lp, ep, "STOP")
                            del entry_prices[ticker]; high_water.pop(ticker,None)
                            open_pos.pop(ticker,None); n_pos=max(0,n_pos-1)
                            buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                        except Exception as ex:
                            dlog.error(f"  STOP order failed {ticker}: {ex}")
                        continue

                    # Take profit
                    if chg >= cfg.get("take_profit_pct", 0.12):
                        _sell_tag = f"LIMIT@${sell_px:.4f}(bid${bid_pm:.4f})" if is_pre_market else f"MKT@${sell_px:.3f}"
                        dlog.info(f"  [TP]   {ticker} {chg:.2%}  {_sell_tag}  P&L: ${(lp-ep)*qty_held:+.2f}")
                        try:
                            submit_order_smart(trading, ticker, qty_held, "SELL",
                                               limit_price=sell_px, extended=is_pre_market)
                            log_trade(ticker, "TP", qty_held, lp, ep, "TP")
                            del entry_prices[ticker]; high_water.pop(ticker,None)
                            open_pos.pop(ticker,None); n_pos=max(0,n_pos-1)
                            buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                        except Exception as ex:
                            dlog.error(f"  TP order failed {ticker}: {ex}")
                        continue

                    # Partial exit — bank half at first target, let rest run
                    if (cfg.get("partial_exit_enabled")
                            and ticker not in partial_exited
                            and chg >= cfg.get("partial_exit_pct", 0.06)
                            and qty_held > 1):
                        sell_qty = max(1, int(qty_held * cfg.get("partial_exit_qty_pct", 0.50)))
                        if sell_qty < qty_held:
                            _sell_tag = f"LIMIT@${sell_px:.4f}(bid${bid_pm:.4f})" if is_pre_market else f"MKT@${lp:.3f}"
                            dlog.info(f"  [PARTIAL] {ticker} up {chg:.2%}  selling {sell_qty}/{qty_held} {_sell_tag}  banking ${(lp-ep)*sell_qty:+.2f}")
                            try:
                                submit_order_smart(trading, ticker, sell_qty, "SELL",
                                                   limit_price=sell_px, extended=is_pre_market)
                                log_trade(ticker, "PARTIAL", sell_qty, lp, ep, "PARTIAL")
                                partial_exited.add(ticker)
                                open_pos[ticker] = qty_held - sell_qty
                                qty_held = open_pos[ticker]
                            except Exception as ex:
                                dlog.error(f"  PARTIAL exit failed {ticker}: {ex}")

                    # Trailing stop
                    # trail_stop_pct is intentionally TIGHTER than stop_loss_pct so
                    # the trail actually protects profits rather than giving back the
                    # same amount the hard stop would have allowed.
                    #
                    # Break-even floor: trigger can never drop below entry price once
                    # the trail is active.  A trade that ran +5% cannot trail-stop into
                    # a loss — worst case you exit at your entry price.
                    trail_act = cfg.get("trail_activation_pct", 0.05)
                    trail_pct = cfg.get("trail_stop_pct", 0.02)   # default 2% (tighter than 4% hard stop)
                    if (hw - ep) / ep >= trail_act:
                        trigger = max(hw * (1 - trail_pct), ep)   # floor = break-even
                        if lp <= trigger:
                            floor_tag = " [BE floor]" if hw * (1 - trail_pct) < ep else ""
                            _sell_tag = f"LIMIT@${sell_px:.4f}(bid${bid_pm:.4f})" if is_pre_market else f"MKT@${sell_px:.3f}"
                            dlog.info(f"  [TRAIL] {ticker}  HW=${hw:.3f}  trigger=${trigger:.3f}{floor_tag}  {_sell_tag}  P&L: ${(lp-ep)*qty_held:+.2f}")
                            try:
                                submit_order_smart(trading, ticker, qty_held, "SELL",
                                                   limit_price=sell_px, extended=is_pre_market)
                                log_trade(ticker, "TRAIL", qty_held, lp, ep, "TRAIL")
                                del entry_prices[ticker]; high_water.pop(ticker,None)
                                open_pos.pop(ticker,None); n_pos=max(0,n_pos-1)
                                buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                            except Exception as ex:
                                dlog.error(f"  TRAIL order failed {ticker}: {ex}")
                            continue

                # Pyramid
                if (cfg.get("pyramid_enabled") and in_pos and ticker not in pyramided
                        and ticker in entry_prices and daily_buys < cfg.get("max_daily_buys",10)):
                    ep  = entry_prices[ticker]
                    gain = (lp - ep) / ep if ep else 0
                    if (gain >= cfg.get("pyramid_gain_pct",0.05)
                            and rsi_v < cfg.get("pyramid_rsi_max",68) and vol_s):
                        _bp2 = acc.get("buying_power", pval)
                        add_qty = int((min(_bp2, pval) * cfg.get("pyramid_size_pct",0.10)) / lp)
                        if add_qty >= 1:
                            ask_pm  = _timed(get_ask_price, data, ticker, timeout=8, default=None, label=f"ask_price:{ticker}") if is_pre_market else None
                            buy_px  = ask_pm * (1 + cfg.get("pre_market_limit_offset_pct", 0.002)) if ask_pm else lp
                            dlog.info(f"  [PYRAMID] {ticker} up {gain:.1%}  adding {add_qty} @ ${buy_px:.3f}")
                            try:
                                submit_order_smart(trading, ticker, add_qty, "BUY",
                                                   limit_price=buy_px, extended=is_pre_market)
                                blended = (qty_held * ep + add_qty * buy_px) / (qty_held + add_qty)
                                entry_prices[ticker] = blended
                                log_trade(ticker, "PYRAMID", add_qty, buy_px, reason="PYRAMID")
                                pyramided.add(ticker); daily_buys += 1
                                buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                                dlog.info(f"  [PYRAMID] Blended entry now ${blended:.3f}")
                            except Exception as ex:
                                dlog.error(f"  PYRAMID failed {ticker}: {ex}")

                # Signal exit — skipped when signal_sell_enabled=False (let stop/target/trail manage)
                if signal == "SELL" and in_pos and cfg.get("signal_sell_enabled", True):
                    ep  = entry_prices.get(ticker, lp)
                    _sell_tag = f"LIMIT@${sell_px:.4f}(bid${bid_pm:.4f})" if is_pre_market else f"MKT@${sell_px:.3f}"
                    dlog.info(f"  [SELL]  {ticker}  {_sell_tag}  P&L: ${(lp-ep)*qty_held:+.2f}")
                    try:
                        submit_order_smart(trading, ticker, qty_held, "SELL",
                                           limit_price=sell_px, extended=is_pre_market)
                        log_trade(ticker, "SELL", qty_held, lp, ep, "SIGNAL")
                        entry_prices.pop(ticker,None); high_water.pop(ticker,None)
                        open_pos.pop(ticker,None); n_pos=max(0,n_pos-1)
                        buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                    except Exception as ex:
                        dlog.error(f"  SELL failed {ticker}: {ex}")

                # Signal entry — skip if user manually closed this ticker (requires manual unlock)
                if signal == "BUY" and not in_pos and daily_loss_halt:
                    dlog.warning(f"  [HALT]   {ticker} BUY skipped — daily loss limit active")
                elif signal == "BUY" and not in_pos and not buys_ok and cfg.get("debug_signals"):
                    _nbb_h, _nbb_m = cfg.get("no_new_buys_before", [10, 0])
                    _nb_h,  _nb_m  = cfg.get("no_new_buys_after",  [15, 30])
                    if (now.hour, now.minute) < (_nbb_h, _nbb_m):
                        dlog.info(f"  [WINDOW] {ticker} BUY skipped — before {_nbb_h:02d}:{_nbb_m:02d} open window")
                elif signal == "BUY" and not in_pos and ticker in state.manually_closed:
                    if cfg.get("debug_signals"):
                        dlog.warning(f"  [LOCKED] {ticker} BUY signal skipped — manually closed, unlock to re-enable")
                elif signal == "BUY" and not in_pos and buys_ok and ticker not in state.manually_closed:
                    _latest = df.iloc[-1]

                    # ── Live RVOL gate at entry ────────────────────────────
                    # Re-check RVOL right now using the bar data already in memory.
                    # The Finviz pre-filter checks RVOL at scan time; this catches
                    # cases where volume has faded by the time the signal fires.
                    _min_rvol    = cfg.get("min_rvol", 2.0)
                    _entry_rvol  = calc_rvol(df)
                    if _entry_rvol.iloc[-1] > 0 and _entry_rvol.iloc[-1] < _min_rvol:
                        dlog.info(f"  [RVOL]  {ticker} BUY skipped — entry RVOL {_entry_rvol.iloc[-1]:.2f}x < min {_min_rvol:.1f}x (volume faded)")
                        continue
                    elif cfg.get("debug_signals") and _entry_rvol.iloc[-1] > 0:
                        dlog.info(f"  [RVOL]  {ticker} entry RVOL {_entry_rvol.iloc[-1]:.2f}x ✓")

                    # ── Multi-timeframe confirmation (optional) ────────────
                    # Fetch 1-min bars and check %R fast+slow both above precheck_threshold.
                    # Skips entry if 1-min momentum has already stalled (false breakout filter).
                    if cfg.get("use_mtf_confirm", False):
                        _mtf_cfg = dict(cfg)
                        _mtf_cfg["bar_timeframe"] = cfg.get("mtf_timeframe", "1Min")
                        _mtf_cfg["bar_count"]     = cfg.get("mtf_bar_count", 60)
                        _mtf_df = _timed(fetch_bars, data, ticker, _mtf_cfg, timeout=8,
                                         default=None, label=f"mtf_bars:{ticker}")
                        _mtf_ok = False
                        if _mtf_df is not None and len(_mtf_df) >= 20:
                            try:
                                _mtf_df = compute_signals(_mtf_df, _mtf_cfg)
                                _mtf_last = _mtf_df.iloc[-1]
                                _mtf_fast = float(_mtf_last.get("rte_fast", -100))
                                _mtf_slow = float(_mtf_last.get("rte_slow", -100))
                                _mtf_thr  = cfg.get("precheck_threshold", -40)
                                _mtf_ok   = _mtf_fast >= _mtf_thr and _mtf_slow >= _mtf_thr
                                dlog.info(f"  [MTF]  {ticker} 1-min f={_mtf_fast:.0f} s={_mtf_slow:.0f} thr={_mtf_thr} → {'✓ CONFIRMED' if _mtf_ok else '✗ REJECTED'}")
                            except Exception as _mtf_e:
                                dlog.debug(f"  [MTF]  {ticker} compute error: {_mtf_e}")
                        else:
                            dlog.debug(f"  [MTF]  {ticker} insufficient 1-min bars — skipping MTF check")
                            _mtf_ok = True   # can't confirm → don't block
                        if not _mtf_ok:
                            continue

                    # ── Conviction multiplier ──────────────────────────────
                    # Count how many of the 4 exhaustion conditions passed
                    _conviction = sum([
                        bool(_latest.get("rte_reversal",  False)),
                        bool(_latest.get("momentum_quality", False)),   # VWAP + RSI-14 in range
                        bool(_latest.get("vol_trend_up",  False)),
                        float(_latest.get("macd_line", 0) or 0) > float(_latest.get("macd_signal_line", 0) or 0),
                    ])
                    # 4/4 → full size, 3/4 → 75%, 2/4 → 50%
                    conviction_mult = {4: 1.0, 3: 0.75, 2: 0.50}.get(_conviction, 0.50)

                    # ── Position sizing ────────────────────────────────────
                    # Cash accounts: size off buying_power (settled cash only — T+2).
                    # Margin accounts: cap at equity to avoid 2× margin over-sizing.
                    _bp        = acc.get("buying_power", pval)
                    _size_base = min(_bp, pval)

                    if cfg.get("use_atr_sizing", False):
                        # Risk a fixed % of equity per trade; size so that
                        # one stop-distance (atr_risk_mult ATRs) equals that risk.
                        _atr_val  = float(_latest.get("atr", 0) or 0)
                        _stop_dist = _atr_val * cfg.get("atr_risk_mult", 1.5)
                        if _atr_val > 0 and _stop_dist > 0:
                            _risk_dollars = _size_base * cfg.get("risk_per_trade_pct", 0.02)
                            qty = int(_risk_dollars / _stop_dist * conviction_mult)
                            dlog.info(f"  [SIZE]  {ticker} ATR={_atr_val:.4f} stop_dist={_stop_dist:.4f} "
                                      f"risk=${_risk_dollars:.2f} → {qty} shares")
                        else:
                            # ATR not ready — fall back to fixed %
                            qty = int((_size_base * cfg.get("position_size_pct", 0.10) * conviction_mult) / lp)
                            dlog.debug(f"  [SIZE]  {ticker} ATR unavailable — using fixed % sizing: {qty} shares")
                    else:
                        base_size = cfg.get("position_size_pct", 0.10)
                        sized_pct = base_size * conviction_mult
                        qty = int((_size_base * sized_pct) / lp)

                    if qty >= 1:
                        # Pre-market: use ask price + offset for limit order
                        ask_pm  = _timed(get_ask_price, data, ticker, timeout=8, default=None, label=f"ask_price:{ticker}") if is_pre_market else None
                        buy_px  = ask_pm * (1 + cfg.get("pre_market_limit_offset_pct", 0.002)) if ask_pm else lp
                        sl  = buy_px * (1 - cfg.get("stop_loss_pct", 0.04))
                        tp  = buy_px * (1 + cfg.get("take_profit_pct", 0.12))
                        _atr_disp = f" ATR={float(_latest.get('atr',0) or 0):.4f}" if cfg.get("use_atr_sizing") else ""
                        mode_tag  = f"LIMIT@${buy_px:.4f}(ask${ask_pm:.4f})" if is_pre_market else f"MKT@${buy_px:.3f}"
                        dlog.info(f"  [BUY]   {qty} x {ticker} {mode_tag}  SL=${sl:.3f}  TP=${tp:.3f}  conviction={_conviction}/4{_atr_disp}")
                        try:
                            # ── Bracket orders (server-side stop + TP) ────
                            # Brackets require regular-hours market orders.
                            # Pre-market falls back to bot-managed exits.
                            _use_bracket = cfg.get("use_bracket_orders", False) and not is_pre_market
                            if _use_bracket:
                                from alpaca.trading.requests import (
                                    MarketOrderRequest, TakeProfitRequest, StopLossRequest)
                                from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
                                _bracket_order = MarketOrderRequest(
                                    symbol=ticker, qty=qty,
                                    side=OrderSide.BUY,
                                    time_in_force=TimeInForce.DAY,
                                    order_class=OrderClass.BRACKET,
                                    take_profit=TakeProfitRequest(limit_price=round(tp, 4)),
                                    stop_loss=StopLossRequest(stop_price=round(sl, 4)),
                                )
                                trading.submit_order(_bracket_order)
                                dlog.info(f"  [BRACKET] {ticker} stop=${sl:.3f} tp=${tp:.3f} — server-managed")
                            else:
                                submit_order_smart(trading, ticker, qty, "BUY",
                                                   limit_price=buy_px, extended=is_pre_market)
                            _buy_si = get_stock_info(ticker)
                            log_trade(ticker, "BUY", qty, buy_px, reason="SIGNAL",
                                streak     = int(_latest.get("rte_boxes_streak", 0)),
                                float_m    = round(_buy_si.get("float_m", 0.0), 2),
                                rvol       = round(float(_latest.get("rvol", 0) or 0), 2),
                                rsi2       = round(float(_latest.get("rmi",  50) or 50), 1),
                                rte_fast   = round(float(_latest.get("rte_fast", -100) or -100), 1),
                                rte_slow   = round(float(_latest.get("rte_slow", -100) or -100), 1),
                                conviction = _conviction,
                            )
                            entry_prices[ticker] = buy_px
                            high_water[ticker]   = buy_px
                            daily_buys += 1; n_pos += 1
                            buys_ok = _check_buys_ok(now, daily_buys, n_pos)
                        except Exception as ex:
                            dlog.error(f"  BUY failed {ticker}: {ex}")

            # Update shared state
            pos_display = get_positions(trading, entry_prices, high_water)
            with state.lock:
                state.entry_prices = dict(entry_prices)
                state.high_water   = dict(high_water)
                state.daily_buys   = daily_buys
                state.positions    = pos_display
                state.last_scan    = now.strftime("%H:%M:%S ET")
                # today_trades is maintained in-memory by log_trade() — no disk read needed

        except Exception as ex:
            dlog.error(f"  Scan error: {ex}")
            with state.lock:
                state.error = str(ex)

        state.stop_event.wait(timeout=cfg.get("scan_interval_sec", 60))

    with state.lock:
        state.running = False
    dlog.info("  Bot stopped.")

# ═══════════════════════════════════════════════════════════
#  TRENDING WATCHLIST  (see trending.py)
# ═══════════════════════════════════════════════════════════

from trending import (
    fetch_stocktwits_trending, fetch_finviz_trending,
    filter_under_five, update_trending_watchlist, trending_updater,
)

# ═══════════════════════════════════════════════════════════
#  AUTO-SCHEDULER
# ═══════════════════════════════════════════════════════════

def auto_scheduler(state):
    """
    Background thread: automatically starts the bot at market_open_at on
    weekdays when auto_schedule is enabled, and lets the existing EOD
    liquidation handle the shutdown.
    """
    import time as _time
    dlog.info("[SCHED] Auto-scheduler running")
    _started_today = None   # date we last auto-started

    while True:
        _time.sleep(20)
        now = datetime.now(ET)
        today = now.date()

        # Weekdays only (Mon=0 … Fri=4)
        if now.weekday() >= 5:
            continue

        with state.lock:
            cfg = dict(state.config)
            running = state.running

        if not cfg.get("auto_schedule"):
            continue

        oh, om = cfg.get("market_open_at", [9, 15])
        eh, em = cfg.get("eod_liquidate_at", [16, 0])

        open_dt = now.replace(hour=oh, minute=om, second=0, microsecond=0)
        eod_dt  = now.replace(hour=eh, minute=em, second=0, microsecond=0)

        in_session = open_dt <= now < eod_dt

        if in_session and not running and _started_today != today:
            dlog.info(f"[SCHED] {now.strftime('%H:%M:%S')} ET — auto-starting bot")
            with state.lock:
                state.running    = True
                state.stop_event = threading.Event()
                state.error      = None
            t = threading.Thread(target=bot_thread, args=(state,), daemon=True)
            t.start()
            _started_today = today

        # Reset so it can fire again next day
        if not in_session and _started_today == today and now >= eod_dt:
            _started_today = None


# ═══════════════════════════════════════════════════════════
#  FASTAPI APP
# ═══════════════════════════════════════════════════════════

app = FastAPI(title="Alpaca Dashboard")


@app.on_event("startup")
async def _startup():
    STATE.today_trades = load_today_trades()
    t = threading.Thread(target=trending_updater, daemon=True)
    t.start()
    s = threading.Thread(target=auto_scheduler, args=(STATE,), daemon=True)
    s.start()
    fh_api_key = STATE.config.get("finnhub_key") or os.getenv("FINNHUB_API_KEY", "")
    if fh_api_key:
        fh_thread = _fh.start_finnhub_stream(fh_api_key, STATE.config.get("tickers", []))
        if fh_thread:
            dlog.info(f"[DASH] Finnhub stream started for {len(STATE.config.get('tickers', []))} tickers")
        else:
            dlog.warning("[DASH] Finnhub stream disabled or failed to start")
    else:
        dlog.warning("[DASH] No Finnhub API key configured; realtime stream will be disabled")
    dlog.info("[DASH] Dashboard ready — trending updater + auto-scheduler started")
    # Open browser automatically (slight delay so the server is fully bound first)
    def _open_browser():
        import time as _t, webbrowser
        _t.sleep(1.5)
        webbrowser.open(f"http://localhost:{PORT}")
    threading.Thread(target=_open_browser, daemon=True).start()

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(_load_dashboard_html().replace("{{BOT_VERSION}}", BOT_VERSION))

@app.get("/events")
async def sse_events(request: Request):
    async def generate():
        last_trade_count = -1
        last_config_hash = None
        tick = 0
        while True:
            if await request.is_disconnected():
                break
            with STATE.lock:
                trade_count  = len(STATE.today_trades)
                config_hash  = id(STATE.config)   # changes when config is replaced on save
            trades_changed = trade_count != last_trade_count
            config_changed = config_hash  != last_config_hash
            # Send full payload on first tick, on config save, or on new trade
            include_static = (tick == 0) or trades_changed or config_changed
            snap = STATE.snapshot(include_static=include_static)
            if include_static:
                last_trade_count = trade_count
                last_config_hash = config_hash
            yield f"data: {json.dumps(snap, default=str)}\n\n"
            tick += 1
            await asyncio.sleep(3)
    return StreamingResponse(generate(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

@app.post("/api/start")
async def api_start():
    with STATE.lock:
        if STATE.running:
            return {"ok": False, "msg": "Already running"}
        STATE.running    = True
        STATE.stop_event = threading.Event()
        STATE.error      = None
    t = threading.Thread(target=bot_thread, args=(STATE,), daemon=True)
    t.start()
    return {"ok": True}

@app.post("/api/stop")
async def api_stop():
    with STATE.lock:
        STATE.stop_event.set()
        STATE.running = False
    return {"ok": True}

@app.post("/api/close/{ticker}")
async def api_close(ticker: str):
    ticker = ticker.upper()
    tc = STATE.trading_client
    if not tc:
        # connect on-demand for manual closes even if bot is stopped
        try:
            tc, _ = connect_alpaca(STATE.config)
        except Exception as e:
            raise HTTPException(400, str(e))
    ok = close_position(tc, ticker)
    if ok:
        with STATE.lock:
            STATE.entry_prices.pop(ticker, None)
            STATE.high_water.pop(ticker, None)
            STATE.positions = [p for p in STATE.positions if p["ticker"] != ticker]
            STATE.manually_closed.add(ticker)   # block auto-rebuy until user unlocks
        dlog.info(f"  [MANUAL] Closed {ticker} — auto-rebuy locked until manually unlocked")
    return {"ok": ok}

@app.post("/api/unlock/{ticker}")
async def api_unlock(ticker: str):
    ticker = ticker.upper()
    with STATE.lock:
        STATE.manually_closed.discard(ticker)
    dlog.info(f"  [UNLOCK] {ticker} auto-rebuy re-enabled")
    return {"ok": True, "ticker": ticker}

@app.post("/api/watching/remove/{ticker}")
async def api_watching_remove(ticker: str):
    ticker = ticker.upper()
    with STATE.lock:
        removed = ticker in STATE.watching
        STATE.watching.pop(ticker, None)
    if removed:
        dlog.info(f"  [ON DECK] {ticker} ← manually dismissed from fast-scan")
        STATE.add_ondeck_event("exit_manual", ticker, "Manually dismissed from On Deck")
    return {"ok": True, "ticker": ticker, "removed": removed}

@app.get("/api/config")
async def api_get_config():
    with STATE.lock:
        return {"ok": True, "config": dict(STATE.config)}


@app.post("/api/validate_paper_to_live")
async def api_validate_paper_to_live():
    """
    Pre-flight validation before switching from paper to live trading.
    Returns checklist of conditions and readiness status.
    
    [P1-03: Paper-to-Live Validation Gate]
    """
    checks = {
        "timestamp": datetime.now(ET).isoformat(),
        "checks": {
            "api_connectivity": False,
            "positions_sync": False,
            "config_valid": False,
            "live_mode_readiness": False,
        },
        "warnings": [],
        "status": "PENDING",  # READY / NOT_READY / ERROR
    }
    
    try:
        with STATE.lock:
            cfg = dict(STATE.config)
            
            # Check 1: API connectivity
            try:
                tc, _ = connect_alpaca(cfg)
                acct = tc.get_account()
                checks["checks"]["api_connectivity"] = True
                checks["account_equity"] = float(acct.equity)
                checks["account_trading_power"] = float(acct.trading_power)
                if acct.cash and float(acct.cash) < 2500:
                    checks["warnings"].append(
                        "Account has less than $2500 cash — day trading buying power may be limited"
                    )
            except Exception as e:
                checks["warnings"].append(f"API connectivity check failed: {str(e)}")
                
            # Check 2: Position sync
            open_positions = len(STATE.positions)
            if open_positions > 0:
                checks["warnings"].append(
                    f"Bot has {open_positions} open position(s). Close all positions before switching to live."
                )
            else:
                checks["checks"]["positions_sync"] = True
                
            # Check 3: Config validation
            if cfg.get("api_key") and cfg.get("secret_key"):
                checks["checks"]["config_valid"] = True
            else:
                checks["warnings"].append("API credentials not configured")
                
            # Check 4: Overall readiness
            if (checks["checks"]["api_connectivity"] and 
                checks["checks"]["positions_sync"] and 
                checks["checks"]["config_valid"]):
                checks["checks"]["live_mode_readiness"] = True
                checks["status"] = "READY"
            else:
                checks["status"] = "NOT_READY"
                
        return checks
        
    except Exception as e:
        checks["status"] = "ERROR"
        checks["error"] = str(e)
        return checks


@app.post("/api/config")
async def api_config(request: Request):
    body = await request.json()
    
    # ─── P1-03: Paper-to-Live Validation Gate ───────────────────────────────────
    # Detect if we're switching from paper trading to live trading
    current_paper = STATE.config.get("paper", True)
    request_paper = body.get("paper", current_paper)
    
    if current_paper and not request_paper:
        # Switching paper → live
        # Require explicit validation before allowing this dangerous transition
        validation_token = body.get("_live_confirmed", False)
        if not validation_token:
            dlog.error("BLOCKED: Paper-to-live switch attempted without validation confirmation")
            return {
                "ok": False,
                "error": "Paper-to-live switch requires validation. Call /api/validate_paper_to_live first and confirm with _live_confirmed=true",
                "required_validation": True,
            }
    
    with STATE.lock:
        # Update allowed keys only
        safe_keys = [
            "paper","api_key","secret_key","tickers",
            "scan_region_top","scan_region_left","scan_region_width","scan_region_height",
            "ema_short","ema_long","rsi_period","rsi_min_buy","rsi_overbought","rsi_sell",
            "volume_surge_mult","use_vwap",
            "position_size_pct","max_positions","max_daily_buys",
            "stop_loss_pct","take_profit_pct",
            "trail_activation_pct","trail_stop_pct",
            "pyramid_enabled","pyramid_gain_pct","pyramid_rsi_max","pyramid_size_pct",
            "use_float_filter","max_float_million","use_rvol","min_rvol",
            "trending_max_price",
            "partial_exit_enabled","partial_exit_pct","partial_exit_qty_pct",
            "auto_schedule","market_open_at",
            "no_new_buys_before","no_new_buys_after","eod_liquidate_at","scan_interval_sec","bar_timeframe",
            # ── Strategy ──────────────────────────────────────
            "strategy",
            "use_rte_exhaustion","use_rmi","use_volume_trending_up","use_macd",
            "rte_side","rte_threshold","rte_avg_ma","rte_min_boxes",
            "rmi_oversold","rmi_ma_fast","rmi_ma_slow","precheck_threshold",
            "macd_fast","macd_slow","macd_signal",
            "tv_chart_url",
            "debug_signals",
            "pre_market_enabled",
            "pre_market_start",
            "pre_market_limit_offset_pct",
            "pre_market_volume_surge_mult",
            # ── Capital protection ─────────────────────────────
            "max_daily_loss_pct",
            "volume_surge_mult",
            "bar_count",
            # ── ATR sizing ─────────────────────────────────────
            "use_atr_sizing","atr_period","risk_per_trade_pct","atr_risk_mult",
            # ── Multi-timeframe confirmation ────────────────────
            "use_mtf_confirm","mtf_timeframe","mtf_bar_count",
            # ── Bracket orders ──────────────────────────────────
            "use_bracket_orders",
            # ── Signal sell ────────────────────────────────────
            "signal_sell_enabled",
            "rte_entry_window","rte_min_supporting",
            "use_rte_exhaustion",
            "micro_float_threshold",
            # ── Momentum Quality (VWAP + RSI-14 filter) ────────
            "momentum_quality_rsi_min","momentum_quality_rsi_max",
            # ── Price extension guard ───────────────────────────
            "rte_max_entry_extension_pct",
        ]
        for k in safe_keys:
            if k in body:
                STATE.config[k] = body[k]
        save_config(dict(STATE.config))   # persist to bot_config.json
        was_running = STATE.running
        if was_running:
            STATE.stop_event.set()
            STATE.running = False
    if was_running:
        await asyncio.sleep(1)
        with STATE.lock:
            STATE.running    = True
            STATE.stop_event = threading.Event()
            STATE.error      = None
        t = threading.Thread(target=bot_thread, args=(STATE,), daemon=True)
        t.start()
    return {"ok": True}


def _get_scanner_region(cfg: dict) -> dict[str, int]:
    return {
        "top": int(cfg.get("scan_region_top", 200) or 200),
        "left": int(cfg.get("scan_region_left", 300) or 300),
        "width": int(cfg.get("scan_region_width", 1200) or 1200),
        "height": int(cfg.get("scan_region_height", 600) or 600),
    }

@app.get("/api/scanner/test")
async def api_scanner_test():
    region = _get_scanner_region(STATE.config)
    try:
        import mss
        import numpy as np
        import cv2
        from screen_ticker_scanner import create_ocr_reader, scan_frame
    except Exception as exc:
        error = str(exc)
        dlog.error(f"[SCANNER TEST] dependency load failed: {error}")
        return {"ok": False, "error": "Scanner dependencies unavailable: " + error}

    try:
        reader = create_ocr_reader()
        with mss.mss() as sct:
            screenshot = sct.grab(region)
        frame = cv2.cvtColor(np.array(screenshot), cv2.COLOR_BGRA2BGR)
        tickers = scan_frame(frame, reader, detail=True)
        timestamp = datetime.now(ET).strftime("%H:%M:%S")
        return {
            "ok": True,
            "timestamp": timestamp,
            "region": region,
            "tickers": tickers,
        }
    except Exception as exc:
        error = str(exc)
        dlog.error(f"[SCANNER TEST] failed: {error}")
        return {"ok": False, "error": error}

@app.get("/api/scanner/read_region")
async def api_scanner_read_region(
    top: int | None = None,
    left: int | None = None,
    width: int | None = None,
    height: int | None = None,
):
    region = _get_scanner_region(STATE.config)
    if top is not None:
        region["top"] = top
    if left is not None:
        region["left"] = left
    if width is not None:
        region["width"] = width
    if height is not None:
        region["height"] = height

    try:
        import mss
        import numpy as np
        import cv2
        from screen_ticker_scanner import create_ocr_reader, scan_frame
    except Exception as exc:
        error = str(exc)
        dlog.error(f"[SCANNER READ] dependency load failed: {error}")
        return {"ok": False, "error": "Scanner dependencies unavailable: " + error}

    try:
        reader = create_ocr_reader()
        with mss.mss() as sct:
            screenshot = sct.grab(region)
        frame = cv2.cvtColor(np.array(screenshot), cv2.COLOR_BGRA2BGR)
        tickers = scan_frame(frame, reader, detail=True)
        timestamp = datetime.now(ET).strftime("%H:%M:%S")
        return {
            "ok": True,
            "timestamp": timestamp,
            "region": region,
            "tickers": tickers,
        }
    except Exception as exc:
        error = str(exc)
        dlog.error(f"[SCANNER READ] failed: {error}")
        return {"ok": False, "error": error}

@app.get("/api/scanner/select_region")
async def api_scanner_select_region():
    try:
        from screen_ticker_scanner import select_screen_region
    except Exception as exc:
        error = str(exc)
        dlog.error(f"[SCANNER SELECT] import failed: {error}")
        return {"ok": False, "error": f"Scanner module unavailable: {error}"}

    try:
        region = select_screen_region()
        if region is None:
            return {"ok": False, "error": "Selection cancelled or no region selected"}
        return {"ok": True, "region": region}
    except Exception as exc:
        error = str(exc)
        dlog.error(f"[SCANNER SELECT] failed: {error}")
        return {"ok": False, "error": error}

@app.get("/api/finnhub_prices")
async def api_finnhub_prices(tickers: str = ""):
    requested = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not requested:
        requested = list(STATE.config.get("tickers", []))
    if not requested:
        return {"ok": False, "error": "No tickers requested"}

    with _fh.FINNHUB_STATE.lock:
        prices_state = dict(_fh.FINNHUB_STATE.prices)
        connected = _fh.FINNHUB_STATE.connected
        subscribed = list(_fh.FINNHUB_STATE.subscribed)

    prices = {}
    for ticker in requested:
        data = prices_state.get(ticker)
        if data is not None and data.get("price") is not None:
            prices[ticker] = float(data["price"])

    if not prices and not connected:
        fh_api_key = STATE.config.get("finnhub_key") or os.getenv("FINNHUB_API_KEY", "")
        if fh_api_key and len(requested) <= 10:
            for ticker in requested:
                if ticker not in prices:
                    result = _fh.fetch_realtime_quote(fh_api_key, ticker)
                    if result.get("ok") and result.get("c") is not None:
                        prices[ticker] = float(result["c"])

    return {
        "ok": True,
        "connected": connected,
        "subscribed": subscribed,
        "prices": prices,
    }

@app.get("/api/test")
async def api_test():
    """Quick connection test — returns account info or error, does not start the bot."""
    try:
        trading, data = connect_alpaca(STATE.config)
        acc = trading.get_account()
        return {
            "ok":     True,
            "paper":  STATE.config.get("paper", True),
            "equity": str(acc.equity),
            "cash":   str(acc.cash),
            "status": str(acc.status),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/api/trades")
async def api_trades():
    return JSONResponse(load_today_trades())


@app.get("/api/analytics")
async def api_analytics(days: int = 90):
    """
    Compute trade statistics from the full trade log.
    Returns summary stats + per-ticker breakdown + equity curve points.
    """
    all_rows = load_all_trades(days=days)
    # Only closed (exit) rows contribute to analytics — filter by action
    exit_actions = {"SELL","STOP","TP","EOD","TRAIL","PARTIAL","EXHAUSTION"}
    exits = [r for r in all_rows if r.get("action","") in exit_actions]

    if not exits:
        return JSONResponse({"trades": [], "stats": {}, "by_ticker": {}, "equity": []})

    trades_out = []
    equity = 1.0
    eq_points = []
    wins = losses = 0
    win_pnls = []
    loss_pnls = []

    for r in exits:
        try:
            pnl_p = float(r.get("pnl_pct") or 0)
        except ValueError:
            pnl_p = 0.0
        won = pnl_p > 0
        if won:
            wins += 1;  win_pnls.append(pnl_p)
        else:
            losses += 1; loss_pnls.append(pnl_p)
        equity *= (1 + pnl_p / 100)
        eq_points.append({"date": r.get("date",""), "time": r.get("time",""),
                           "equity": round(equity, 4)})
        trades_out.append({
            "date":       r.get("date",""),
            "time":       r.get("time",""),
            "ticker":     r.get("ticker",""),
            "action":     r.get("action",""),
            "pnl_pct":    pnl_p,
            "pnl_dollars":r.get("pnl_dollars",""),
            "streak":     r.get("streak",""),
            "float_m":    r.get("float_m",""),
            "rvol":       r.get("rvol",""),
            "rsi2":       r.get("rsi2",""),
            "conviction": r.get("conviction",""),
            "reason":     r.get("reason",""),
        })

    total = wins + losses
    win_rate = round(wins / total * 100, 1) if total else 0
    avg_win  = round(sum(win_pnls)  / len(win_pnls),  2) if win_pnls  else 0
    avg_loss = round(sum(loss_pnls) / len(loss_pnls), 2) if loss_pnls else 0
    expectancy = round((win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss), 2)

    # Max drawdown
    peak = 1.0; max_dd = 0.0
    for pt in eq_points:
        e = pt["equity"]
        if e > peak: peak = e
        dd = (peak - e) / peak
        if dd > max_dd: max_dd = dd

    # Per-ticker breakdown
    by_ticker: dict = {}
    for t in trades_out:
        tk = t["ticker"]
        by_ticker.setdefault(tk, {"trades":0,"wins":0,"total_pnl":0.0,"pnls":[]})
        by_ticker[tk]["trades"]    += 1
        by_ticker[tk]["wins"]      += int(t["pnl_pct"] > 0)
        by_ticker[tk]["total_pnl"] += t["pnl_pct"]
        by_ticker[tk]["pnls"].append(t["pnl_pct"])
    for tk in by_ticker:
        v = by_ticker[tk]
        v["win_rate"]   = round(v["wins"]/v["trades"]*100, 1) if v["trades"] else 0
        v["avg_pnl"]    = round(v["total_pnl"]/v["trades"], 2) if v["trades"] else 0
        v["total_pnl"]  = round(v["total_pnl"], 2)
        del v["pnls"]

    return JSONResponse({
        "trades":    trades_out,
        "stats": {
            "total":       total,
            "wins":        wins,
            "losses":      losses,
            "win_rate":    win_rate,
            "avg_win":     avg_win,
            "avg_loss":    avg_loss,
            "expectancy":  expectancy,
            "total_pnl":   round((equity - 1) * 100, 2),
            "max_drawdown":round(max_dd * 100, 2),
            "days":        days,
        },
        "by_ticker": by_ticker,
        "equity":    eq_points,
    })


@app.get("/api/check_signal/{ticker}")
async def api_check_signal(ticker: str):
    """
    Run the current strategy on a single ticker and return indicator values.
    Used by the dashboard's "test strategy" button on ticker badges.
    Connects to Alpaca on-demand if bot isn't running.
    """
    cfg = dict(STATE.config)
    
    # Use existing data client if available, otherwise connect on-demand
    dc = STATE.data_client
    if dc is None:
        try:
            _, dc = connect_alpaca(cfg)
        except Exception as e:
            return JSONResponse({"ok": False, "error": f"Cannot connect to Alpaca: {e}"}, status_code=503)

    t = ticker.strip().upper()
    try:
        df = await asyncio.get_event_loop().run_in_executor(
            _FETCH_EXECUTOR, fetch_bars, dc, t, cfg
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return JSONResponse({"ok": False, "error": f"No bar data for {t}"})

        df = compute_signals(df, cfg)
        latest = df.iloc[-1].to_dict()

        result = {
            "ok":       True,
            "ticker":   t,
            "bars":     len(df),
            "close":    round(float(latest.get("close", 0)), 4),
            "signal":   str(latest.get("signal", "HOLD")),
            "strategy": cfg.get("strategy", "exhaustion"),
        }

        strat = cfg.get("strategy", "exhaustion")
        if strat == "exhaustion":
            rmi_thresh = cfg.get("rmi_oversold", 10)
            min_boxes  = cfg.get("rte_min_boxes", 2)
            rmi_v      = float(latest.get("rmi", 50))
            result.update({
                "rte_extreme":      bool(latest.get("rte_extreme",       False)),
                "rte_reversal":     bool(latest.get("rte_reversal",      False)),
                "rte_boxes_streak": int(latest.get("rte_boxes_streak",   0)),
                "rte_boxes_total":  int(latest.get("rte_boxes_completed", 0)),
                "rte_fast":        round(float(latest.get("rte_fast", -100)), 1),
                "rte_slow":        round(float(latest.get("rte_slow", -100)), 1),
                "rte_min_boxes":   min_boxes,
                "rsi2":            round(rmi_v, 1),
                "rsi2_pass":       bool(latest.get("rmi_signal", False)),
                "rsi2_threshold":  rmi_thresh,
                "vol_trend_up":    bool(latest.get("vol_trend_up",   False)),
                "macd_bull":       bool(latest.get("macd_bull",      False)),
                "rsi":             round(float(latest.get("rsi", 0)), 1),
                "ema_short":       round(float(latest.get("ema_short", 0)), 4),
                "ema_long":        round(float(latest.get("ema_long",  0)), 4),
            })
        elif strat == "ema_crossover":
            result.update({
                "rsi":       round(float(latest.get("rsi", 0)), 1),
                "ema_short": round(float(latest.get("ema_short", 0)), 4),
                "ema_long":  round(float(latest.get("ema_long",  0)), 4),
                "vol_surge": bool(latest.get("vol_surge", False)),
                "cross_up":  bool(latest.get("cross_up",  False)),
            })

        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@app.post("/api/manual_buy")
async def api_manual_buy(request: Request):
    """
    Execute a manual market buy order.
    Body: { "ticker": "AAPL", "qty": 10 }
    """
    try:
        body   = await request.json()
        ticker = str(body.get("ticker", "")).strip().upper()
        qty    = int(body.get("qty", 0))
        if not ticker:
            return JSONResponse({"ok": False, "error": "ticker required"}, status_code=400)
        if qty <= 0:
            return JSONResponse({"ok": False, "error": "qty must be > 0"}, status_code=400)

        tc = STATE.trading_client
        if tc is None:
            return JSONResponse({"ok": False, "error": "Bot not connected — start the bot first"}, status_code=503)

        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums    import OrderSide, TimeInForce
        order = tc.submit_order(MarketOrderRequest(
            symbol=ticker,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        ))
        dlog.info(f"  [MANUAL BUY] {qty} x {ticker}  order_id={order.id}")
        return JSONResponse({"ok": True, "ticker": ticker, "qty": qty, "order_id": str(order.id)})
    except Exception as e:
        dlog.error(f"  [MANUAL BUY] Failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/watching")
async def api_watching():
    """Return current fast-scan watch set — polled every second by the On Deck panel."""
    with STATE.lock:
        return JSONResponse({"watching": _sanitize_floats({t: dict(v) for t, v in STATE.watching.items()})})

@app.get("/api/trending")
async def api_trending():
    """Manually trigger a trending refresh and return the new list."""
    tickers = await asyncio.get_event_loop().run_in_executor(None, update_trending_watchlist)
    return JSONResponse({
        "tickers": tickers,
        "sources": dict(STATE.trending_sources),
        "prices":  dict(STATE.trending_prices),
        "count":   len(tickers),
        "updated": STATE.trending_updated,
    })

# ═══════════════════════════════════════════════════════════
#  DASHBOARD HTML  (served from dashboard.html)
# ═══════════════════════════════════════════════════════════

_DASHBOARD_HTML_PATH = Path(__file__).parent / 'dashboard.html'

def _load_dashboard_html() -> str:
    """Read dashboard.html from disk; falls back to bundled string if missing."""
    try:
        return _DASHBOARD_HTML_PATH.read_text(encoding='utf-8')
    except FileNotFoundError:
        raise RuntimeError(
            'dashboard.html not found — make sure it is next to alpaca_dashboard.py'
        )


# ═══════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════╗
║   Alpaca Momentum Bot  —  Dashboard      ║
║   http://localhost:{PORT}                ║
║   Ctrl+C to stop                         ║
╚══════════════════════════════════════════╝
""")
    # Suppress access-log noise for high-frequency polling endpoints
    class _SuppressPollingPaths(logging.Filter):
        _MUTE = {"/api/watching", "/api/trending"}
        def filter(self, record):
            msg = record.getMessage()
            return not any(p in msg for p in self._MUTE)
    logging.getLogger("uvicorn.access").addFilter(_SuppressPollingPaths())

    # Pre-load today's trades
    STATE.today_trades = load_today_trades()
    uvicorn.run("alpaca_dashboard:app", host="0.0.0.0", port=PORT,
                log_level="info", reload=True)
