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

import asyncio, csv, json, logging, os, sys, threading, time
import urllib.request, urllib.error
try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False
from collections import deque
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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
CONFIG_FILE    = Path(__file__).parent / "bot_config.json"
PORT           = 8888

# ── Default config (mirrors alpaca_stocks_bot.py) ──────────
DEFAULT_CONFIG = {
    "api_key":    os.getenv("ALPACA_API_KEY",    "PKKFEWQ327DQ4CB5A26P5FBLJJ"),
    "secret_key": os.getenv("ALPACA_SECRET_KEY", "9vVGjo7HqxZTqNXrX6g9MCg12aSbQK38vbzPPjNQwYaj"),
    "paper": True,
    "tickers": [
        
    ],
    "ema_short": 8, "ema_long": 21,
    "rsi_period": 14, "rsi_min_buy": 45, "rsi_overbought": 65, "rsi_sell": 75,
    "volume_surge_mult": 1.5, "use_vwap": True,
    "position_size_pct": 0.10, "max_positions": 10, "max_daily_buys": 10,
    "stop_loss_pct": 0.04, "take_profit_pct": 0.12,
    "trail_activation_pct": 0.05, "trail_stop_pct": 0.02,  # tighter than hard stop — protects gains
    "pyramid_enabled": True, "pyramid_gain_pct": 0.05,
    "pyramid_rsi_max": 68,   "pyramid_size_pct": 0.10,
    "no_new_buys_before": [10,  0],   # no new buys before 10:00 AM — skip open chop
    "no_new_buys_after":  [15, 30],
    "eod_liquidate_at":  [16,  0],  # 4:00 PM ET
    "scan_interval_sec": 60,
    "bar_timeframe": "1Min", "bar_count": 800,
    # ── Float filter ──────────────────────────────────
    "use_float_filter":  True,
    "max_float_million": 50,       # skip stocks with float > 50M shares
    "micro_float_threshold": 10,   # On Deck: floats ≤ this (M) get ⚡ badge + top sort priority
    # ── Daily RVOL filter ─────────────────────────────
    "use_rvol":          True,
    "min_rvol":          2.0,      # today's vol must be >= 2x expected by this time
    # ── Partial exit ──────────────────────────────────
    "partial_exit_enabled": True,
    "partial_exit_pct":     0.06,  # take partial at +6%
    "partial_exit_qty_pct": 0.50,  # sell 50% of position
    # ── Auto-scheduler ────────────────────────────────
    "auto_schedule":        True,  # auto-start at market_open_at, auto-stop at eod
    "market_open_at":       [9, 15],  # 9:15 AM ET
    # ── StockTwits + Finviz price filter ───────────────
    "trending_max_price":   5.0,   # show trending tickers strictly UNDER this price (e.g. 5 → <$5.00)
    # ── Williams %R Exhaustion ─────────────────────────
    "wr_length":            14,
    "wr_oversold":         -80,
    "wr_overbought":       -20,
    # ── CM RSI Lower ───────────────────────────────────
    "cm_rsi_length":        14,
    "cm_rsi_oversold":      30,
    "cm_rsi_overbought":    70,
    # ── OBV Oscillator ─────────────────────────────────
    "obv_length":           20,
    # ── Volume Trend ───────────────────────────────────
    "vol_trend_short":      10,
    "vol_trend_long":       50,
    # ── Strategy ───────────────────────────────────────
    # Options: "exhaustion" (default), "ema_crossover", "macd"
    "strategy":              "exhaustion",
    "use_rte_exhaustion":    True,
    "use_rmi":               True,       # CM RSI-2 (Larry Connors) bullish signal gate
    "use_volume_trending_up": True,
    "use_macd":              True,
    "rte_side":              "red",      # "red" = overbought, "blue" = oversold
    "rte_threshold":         20,         # distance from 0 to flag extreme (applied as -threshold)
    "rte_avg_ma":            3,          # "Average Formula MA" — composite EMA
    "rte_min_boxes":         2,          # streak boxes required for buy (1=On Deck, 2=Buy eligible)
    # ── Entry relaxation ───────────────────────────────
    # The exhaustion reversal is a single-bar event. rte_entry_window allows
    # a buy up to N bars AFTER the reversal while conditions are still aligning.
    # rte_min_supporting = how many of [rmi, volume, macd] must also pass.
    # Setting to 1 means "reversal + any one confirmation" — good starting point.
    # Setting to 2 is tighter; 3 is equivalent to the old all-AND behaviour.
    "rte_entry_window":      10,         # bars after reversal that entry is still valid
    "rte_min_supporting":    1,          # min of [rmi, vol, macd] that must confirm
    # ── CM RSI-2 (Larry Connors RSI-2 Strategy) ────────
    "rmi_oversold":          10,         # RSI-2 must be below this (deeply oversold)
    "rmi_ma_fast":           20,         # short-term SMA for pullback check (< this = pullback)
    "rmi_ma_slow":           200,        # long-term SMA for trend filter (> this = uptrend)
    # ── Pre-check fast-scan ─────────────────────────────
    "precheck_threshold":    -40,        # both %R lines must exceed this to enter fast-scan
    "macd_fast":             12,
    "macd_slow":             26,
    "macd_signal":           9,
    # ── Pre-market ─────────────────────────────────────
    "pre_market_enabled":    False,  # allow scanning / trading before 9:30 AM ET
    "pre_market_start":      [4, 0], # earliest session start (ET)
    "pre_market_limit_offset_pct": 0.002,  # add 0.2% above ask for better fill probability
    # Volume gate during pre-market uses a lower multiplier because the 20-bar rolling
    # average is dominated by yesterday's regular-hours volume (10-100x higher than
    # pre-market bars).  0.3 = "show me at least 30% of the rolling-avg bar size"
    # which still enforces "something is actually trading" without blocking every stock.
    "pre_market_volume_surge_mult": 0.3,
    # ── ATR-based position sizing ──────────────────────
    # When enabled, risk a fixed % of equity per trade rather than a fixed % of equity per share.
    # qty = (equity × risk_per_trade_pct) / (ATR × atr_risk_mult)
    # atr_risk_mult sets how many ATRs your stop is away (1.5 = stop is 1.5 ATR below entry).
    # Fallback to position_size_pct if ATR not available.
    "use_atr_sizing":        False,
    "atr_period":            14,
    "risk_per_trade_pct":    0.02,   # risk 2% of equity per trade
    "atr_risk_mult":         1.5,    # stop placed 1.5 ATR below entry
    # ── Multi-timeframe confirmation ────────────────────
    # Before entering, check that the 1-min bars also show %R exhaustion above
    # precheck_threshold on both fast and slow lines.  Filters false breakouts.
    "use_mtf_confirm":       False,
    "mtf_timeframe":         "1Min",
    "mtf_bar_count":         60,     # 1-min bars to fetch for confirmation check
    # ── Bracket orders ──────────────────────────────────
    # Submit stop-loss + take-profit as a server-side bracket at entry.
    # Alpaca manages exits even if the bot goes offline.
    # Note: brackets not supported for extended-hours orders — pre-market falls back to bot-managed.
    "use_bracket_orders":    False,
    # ── Capital Protection ─────────────────────────────
    "max_daily_loss_pct":    0.05,   # halt all new buys if equity drops 5% from day open
    # ── Debug ──────────────────────────────────────────
    "debug_signals":         True,  # log why each buy/sell signal was accepted or rejected
    # ── TradingView ────────────────────────────────────
    "tv_chart_url":          "https://www.tradingview.com/chart/x04Gfcu8/",
    # ── Finviz momentum screener ────────────────────────
    # Second trending source alongside StockTwits.
    # Uses finvizfinance (pip install finvizfinance).
    # Filters map 1-to-1 to Finviz screener dropdown labels.
    "finviz_enabled":        True,
    "finviz_exchange":       "NASDAQ",      # "NASDAQ", "NYSE", "AMEX", or "" for all
    "finviz_performance":    "Week Up",     # "Week Up", "Month Up", "Today Up 3%", etc.
    "finviz_rel_volume":     "Over 2",      # relative volume vs average — "Over 1.5" / "Over 2" / "Over 3"
    "finviz_avg_volume":     "Over 500K",   # minimum absolute daily volume for real liquidity
    # Low float: small-float stocks amplify moves — fewer shares = bigger % swings.
    # Maps to Finviz "Float" filter.  "" = disabled.
    "finviz_float":          "Under 20M",  # "Under 1M" / "Under 5M" / "Under 10M" / "Under 20M" / "Under 50M"
    # Signal: finvizfinance does NOT expose the Finviz Signal preset as a screener filter.
    # Must be "" — reserved if the library ever adds support.
    "finviz_signal":         "",
    "finviz_include_news":   True,          # log top market news headlines from Finviz
}

# ═══════════════════════════════════════════════════════════
#  CONFIG PERSISTENCE
# ═══════════════════════════════════════════════════════════

def save_config(cfg: dict):
    """Atomically write config — write to .tmp then rename so a crash never corrupts the file."""
    import tempfile
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(dir=CONFIG_FILE.parent, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump(cfg, f, indent=2)
            Path(tmp_path).replace(CONFIG_FILE)
        except Exception:
            try: os.unlink(tmp_path)
            except: pass
            raise
    except Exception as e:
        print(f"[CFG] Failed to save config: {e}")

def load_config() -> dict:
    """
    Load saved config from bot_config.json and merge with DEFAULT_CONFIG
    so new keys added in code are always present.
    """
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r") as f:
                saved = json.load(f)
            cfg.update(saved)
            print(f"[CFG] Loaded config from {CONFIG_FILE}")
        except Exception as e:
            print(f"[CFG] Failed to load config ({e}) — using defaults")
    else:
        print("[CFG] No saved config found — using defaults")
    return cfg


# ═══════════════════════════════════════════════════════════
#  STOCK INFO CACHE  (float + avg daily volume via yfinance)
# ═══════════════════════════════════════════════════════════

_stock_info_cache: dict = {}   # ticker -> {float_m, avg_vol, fetched_date}
_stock_info_lock  = threading.Lock()

def get_stock_info(ticker: str) -> dict:
    """
    Return {float_m: float in millions, avg_vol: average daily volume}.
    Results are cached for the calendar day.
    Falls back to neutral values (won't filter anything) if yfinance unavailable.
    """
    today = date.today()
    with _stock_info_lock:
        cached = _stock_info_cache.get(ticker)
        if cached and cached.get("fetched_date") == today:
            return cached

    if not _YF_AVAILABLE:
        result = {"float_m": 0.0, "avg_vol": 0, "fetched_date": today}
        with _stock_info_lock:
            _stock_info_cache[ticker] = result
        return result

    try:
        info     = yf.Ticker(ticker).info
        float_sh = info.get("floatShares") or info.get("sharesOutstanding") or 0
        avg_vol  = (info.get("averageVolume10days")
                    or info.get("averageDailyVolume10Day")
                    or info.get("averageVolume") or 0)
        result = {
            "float_m":      round(float_sh / 1_000_000, 2) if float_sh else 0.0,
            "avg_vol":      int(avg_vol),
            "fetched_date": today,
        }
    except Exception:
        result = {"float_m": 0.0, "avg_vol": 0, "fetched_date": today}

    with _stock_info_lock:
        _stock_info_cache[ticker] = result
    return result


def prefetch_stock_info(tickers: list, max_workers: int = 10):
    """
    Background-friendly batch prefetch for a list of tickers.
    Uses a thread pool so 570 tickers complete in ~30-60 s instead of 10 min.
    """
    if not _YF_AVAILABLE or not tickers:
        return
    from concurrent.futures import ThreadPoolExecutor
    today = date.today()
    # Only fetch tickers not already cached for today
    needed = [t for t in tickers
              if _stock_info_cache.get(t, {}).get("fetched_date") != today]
    if not needed:
        return
    dlog.info(f"[INFO] Prefetching float/RVOL data for {len(needed)} tickers …")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        list(ex.map(get_stock_info, needed))
    dlog.info(f"[INFO] Stock info prefetch complete ({len(needed)} tickers cached)")


# ═══════════════════════════════════════════════════════════
#  SHARED STATE
# ═══════════════════════════════════════════════════════════

class BotState:
    def __init__(self):
        self.lock          = threading.Lock()
        self.running       = False
        self.stop_event    = threading.Event()
        self.config        = DEFAULT_CONFIG.copy()
        self.account       = {}
        self.positions     = []        # list of dicts
        self.entry_prices  = {}
        self.high_water    = {}
        self.pyramided     = set()
        self.daily_buys    = 0
        self.last_scan     = None
        self.log_lines     = deque(maxlen=300)
        self.trading_client = None
        self.data_client    = None
        self.error             = None
        self.today_trades      = []    # trades from CSV today
        self.trending_tickers  = []    # merged under-price list (StockTwits + Finviz)
        self.trending_sources  = {}    # {ticker: "stocktwits" | "finviz" | "both"}
        self.trending_prices   = {}    # {ticker: float price} captured during price filter
        self.trending_updated  = None  # timestamp of last trending fetch
        self.manually_closed   = set() # tickers manually closed — block auto-rebuy until unlocked
        self.watching          = {}    # {ticker: {"rte_fast": float, "rte_slow": float}} — fast-scan set
        self.ondeck_events     = deque(maxlen=30)  # recent On Deck entry/exit events
        self.partial_exited    = set() # tickers that have taken a partial exit today (fast-watch mirror)
        self.fast_closing      = set() # tickers currently being closed by fast-watch (prevents double-sell)

    def add_log(self, level: str, msg: str):
        ts = datetime.now(ET).strftime("%H:%M:%S")
        with self.lock:
            self.log_lines.append({"ts": ts, "level": level, "msg": msg})

    def add_ondeck_event(self, direction: str, ticker: str, reason: str, conditions: dict = None):
        """direction: 'enter' | 'exit_buy' | 'exit_fail' | 'exit_manual' | 'exit_position'"""
        ts = datetime.now(ET).strftime("%H:%M:%S")
        with self.lock:
            self.ondeck_events.append({
                "ts":         ts,
                "direction":  direction,
                "ticker":     ticker,
                "reason":     reason,
                "conditions": conditions or {},
            })

    def snapshot(self, include_static: bool = False) -> dict:
        """
        Return the current state.
        include_static=True  → full payload including config + trades (sent once on connect + on change)
        include_static=False → lightweight payload for 3-second ticks (positions, logs, account only)
        """
        with self.lock:
            data = {
                "running":        self.running,
                "paper":          self.config.get("paper", True),
                "account":        dict(self.account),
                "positions":      list(self.positions),
                "entry_prices":   dict(self.entry_prices),
                "high_water":     dict(self.high_water),
                "daily_buys":     self.daily_buys,
                "max_daily_buys": self.config.get("max_daily_buys", 10),
                "last_scan":      self.last_scan,
                "log_lines":      list(self.log_lines)[-300:],
                "error":          self.error,
                "manually_closed":  sorted(self.manually_closed),
                "watching":         {t: dict(v) for t, v in self.watching.items()},
                "ondeck_events":    list(self.ondeck_events)[-10:],
            }
            if include_static:
                data["config"]           = dict(self.config)
                data["today_trades"]     = list(self.today_trades)
                data["trending_tickers"] = list(self.trending_tickers)
                data["trending_sources"] = dict(self.trending_sources)
                data["trending_prices"]  = dict(self.trending_prices)
                data["trending_updated"] = self.trending_updated
            return data

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

# ═══════════════════════════════════════════════════════════
#  INDICATOR CALCULATIONS
# ═══════════════════════════════════════════════════════════

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def vwap_calc(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    if hasattr(df.index, "date"):
        day = pd.Series(df.index.date, index=df.index)
    else:
        day = pd.Series([d.date() for d in df.index], index=df.index)
    cum_tp  = (typical * df["volume"]).groupby(day).cumsum()
    cum_vol = df["volume"].groupby(day).cumsum()
    return cum_tp / cum_vol.replace(0, np.nan)

# ── Williams %R ────────────────────────────────────────────
def williams_pr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    """
    Williams Percent Range.
    Range: 0 (overbought) to -100 (oversold).
    Classic thresholds: overbought > -20, oversold < -80.
    """
    hh = high.rolling(length).max()
    ll = low.rolling(length).min()
    return -100 * (hh - close) / (hh - ll).replace(0, np.nan)


def compute_percent_r_exhaustion(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Williams %R Exhaustion — two-line version matching the TradingView indicator.

    White (fast) line : EMA-7 of W%R(21)
    Blue  (slow) line : EMA-3 of W%R(112)

    extreme  = BOTH lines are simultaneously ≥ -threshold (default -20).
               Requiring both to agree gives far fewer false signals than using
               the triple-smoothed composite alone.
    reversal = one bar after extreme ends (both lines drop below threshold) — entry signal.
    """
    df = df.copy()

    s_pr = williams_pr(df["high"], df["low"], df["close"], 21)
    l_pr = williams_pr(df["high"], df["low"], df["close"], 112)
    s_percentR = s_pr.ewm(span=7, adjust=False).mean()   # fast / white line
    l_percentR = l_pr.ewm(span=3, adjust=False).mean()   # slow / blue  line

    # Keep composite for reference (still used by pre-check proximity check)
    avg_ma       = cfg.get("rte_avg_ma", 3)
    avg_percentR = (s_percentR + l_percentR) / 2
    final        = avg_percentR.ewm(span=avg_ma, adjust=False).mean()

    threshold = abs(cfg.get("rte_threshold", 20))   # always positive — config may store signed
    side      = cfg.get("rte_side", "red").lower()

    if side == "red":
        # Both lines must be in the overbought zone — tighter, fewer false positives
        extreme = (s_percentR >= -threshold) & (l_percentR >= -threshold)
    else:
        extreme = (s_percentR <= (-100 + threshold)) & (l_percentR <= (-100 + threshold))

    reversal = (~extreme) & extreme.shift(1).fillna(False)
    boxes    = reversal.cumsum().fillna(0).astype(int)

    # ── Streak counter ────────────────────────────────────────────────────────
    # Counts consecutive boxes across the CURRENT active setup sequence.
    #
    # streak = 1 → first box fired  → ticker eligible for On Deck
    # streak = 2 → second box fired → ticker eligible for Buy (with other signals)
    #
    # The streak increments on every reversal (box completion) and only resets
    # when BOTH %R lines fall below the failure threshold — meaning the setup has
    # genuinely broken down and the pattern needs to start over.
    # It does NOT reset on extreme re-entry (each new zone entry is part of
    # building the consecutive pattern, not a restart).
    _fail_thr  = cfg.get("precheck_threshold", -40)
    setup_fail = (s_percentR < _fail_thr) & (l_percentR < _fail_thr)
    streak_arr = np.zeros(len(df), dtype=int)
    _count     = 0
    for _i in range(len(df)):
        if bool(setup_fail.iloc[_i]):
            _count = 0          # both lines below threshold — setup abandoned
        if bool(reversal.iloc[_i]):
            _count += 1         # completed a box — increment streak
        streak_arr[_i] = _count
    boxes_streak = pd.Series(streak_arr, index=df.index)

    # Expose individual lines so the pre-check and scan log can use them
    df["rte_fast"]            = s_percentR   # white line (fast, 21-bar)
    df["rte_slow"]            = l_percentR   # blue  line (slow, 112-bar)
    df["rte_composite"]       = final        # triple-smoothed composite (reference only)
    df["rte_extreme"]         = extreme
    df["rte_reversal"]        = reversal
    df["rte_boxes_completed"] = boxes
    df["rte_boxes_streak"]    = boxes_streak  # consecutive boxes in current episode
    return df

# ── CM RSI-2 (ChrisMoody / Larry Connors RSI-2 Strategy) ─────
def compute_rmi(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    ChrisMoody implementation of Larry Connors' RSI-2 Strategy.

    Pine Script (verbatim):
        up   = rma(max(change(src), 0), 2)
        down = rma(-min(change(src), 0), 2)
        rsi  = down==0 ? 100 : up==0 ? 0 : 100 - (100/(1+up/down))
        col  = close > ma200 and close < ma5 and rsi < 10  ? lime  (bullish)
             : close < ma200 and close > ma5 and rsi > 90  ? red   (bearish)

    `rma(x, n)` in Pine Script = Wilder's Moving Average = EMA with alpha = 1/n.
    For period 2, alpha = 0.5, which is what this uses.

    Columns added:
        rmi          : RSI-2 value (0–100)
        rmi_ma_fast  : SMA(rmi_ma_fast period, default 5)   — pullback MA
        rmi_ma_slow  : SMA(rmi_ma_slow period, default 200) — trend MA
        rmi_signal   : True when bullish conditions met:
                         close > SMA(200)  ← long-term uptrend
                       AND close < SMA(5)  ← short-term pullback
                       AND rsi < rmi_oversold (default 10)

    Config keys (all optional):
        rmi_oversold : oversold threshold (default 10)
        rmi_ma_fast  : short pullback MA  (default  5)
        rmi_ma_slow  : long trend MA      (default 200)
    """
    oversold = cfg.get("rmi_oversold", 10)
    ma_fast  = cfg.get("rmi_ma_fast",   5)
    ma_slow  = cfg.get("rmi_ma_slow", 200)

    # Wilder's Moving Average with alpha = 1/2  →  rma(x, 2) in Pine Script
    delta = df["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=0.5, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=0.5, adjust=False).mean()
    rsi2  = np.where(
        loss == 0, 100,
        np.where(gain == 0, 0, 100 - (100 / (1 + gain / loss)))
    )
    df["rmi"] = pd.Series(rsi2, index=df.index)

    # Trend-context MAs (qualify the signal exactly as in the Pine Script)
    df["rmi_ma_fast"] = df["close"].rolling(ma_fast).mean()    # SMA-5
    df["rmi_ma_slow"] = df["close"].rolling(ma_slow).mean()    # SMA-200

    # Bullish (lime) signal: uptrend + pullback + deeply oversold RSI-2
    above_slow = df["close"] > df["rmi_ma_slow"]
    below_fast = df["close"] < df["rmi_ma_fast"]
    deeply_os  = df["rmi"]   < oversold
    df["rmi_signal"] = above_slow & below_fast & deeply_os
    return df


# ── OBV Oscillator ──────────────────────────────────────────
def compute_obv_oscillator(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    On Balance Volume Oscillator = OBV − EMA(OBV, length).
    Positive oscillator → buying pressure dominates.
    Zero-line crossovers signal momentum shifts.

    obv_bull : OBV oscillator crosses from negative to positive → bullish
    obv_bear : OBV oscillator crosses from positive to negative → bearish
    obv_rising : oscillator is positive and increasing (strong accumulation)
    """
    length = cfg.get("obv_length", 20)

    direction   = np.sign(df["close"].diff()).fillna(0)
    df["obv"]   = (direction * df["volume"]).cumsum()
    df["obv_ema"] = ema(df["obv"], length)
    df["obv_osc"] = df["obv"] - df["obv_ema"]

    df["obv_bull"]   = (df["obv_osc"].shift(1) < 0) & (df["obv_osc"] >= 0)
    df["obv_bear"]   = (df["obv_osc"].shift(1) > 0) & (df["obv_osc"] <= 0)
    df["obv_rising"] = (df["obv_osc"] > 0) & (df["obv_osc"] > df["obv_osc"].shift(1))
    return df


# ── Volume Trending Up ──────────────────────────────────────
def compute_volume_trending_up(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Detects whether volume is in a meaningful uptrend.

    vol_trend_up      : short-term volume MA is above long-term volume MA (accumulation phase)
    vol_trend_strong  : volume is above its short MA AND short MA > long MA (confirmed surge into trend)
    vol_trend_cross   : short vol MA just crossed above long vol MA (momentum turning point)
    """
    short = cfg.get("vol_trend_short", 5)
    long_ = cfg.get("vol_trend_long",  20)

    df["vol_ma_short"] = df["volume"].rolling(short).mean()
    df["vol_ma_long"]  = df["volume"].rolling(long_).mean()

    df["vol_trend_up"]     = df["vol_ma_short"] > df["vol_ma_long"]
    df["vol_trend_strong"] = (df["volume"] > df["vol_ma_short"]) & df["vol_trend_up"]
    df["vol_trend_cross"]  = (
        (df["vol_ma_short"].shift(1) <= df["vol_ma_long"].shift(1)) &
        (df["vol_ma_short"]           >  df["vol_ma_long"])
    )
    return df


# ── MACD ────────────────────────────────────────────────────
def compute_macd(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    MACD = EMA(fast) − EMA(slow).
    Signal = EMA(MACD, signal_period).
    Histogram = MACD − Signal.

    macd_bull : MACD line crosses UP through the signal line (bullish crossover)
    macd_bear : MACD line crosses DOWN through the signal line (bearish crossover)
    """
    fast   = cfg.get("macd_fast",   12)
    slow   = cfg.get("macd_slow",   26)
    sig    = cfg.get("macd_signal",  9)

    df["macd_line"]   = ema(df["close"], fast) - ema(df["close"], slow)
    df["macd_signal_line"] = ema(df["macd_line"], sig)
    df["macd_hist"]   = df["macd_line"] - df["macd_signal_line"]
    df["macd_bull"]   = (
        (df["macd_line"] >  df["macd_signal_line"]) &
        (df["macd_line"].shift(1) <= df["macd_signal_line"].shift(1))
    )
    df["macd_bear"]   = (
        (df["macd_line"] <  df["macd_signal_line"]) &
        (df["macd_line"].shift(1) >= df["macd_signal_line"].shift(1))
    )
    return df


def compute_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Route to the correct signal strategy based on cfg["strategy"].

    "ema_crossover" — EMA 8/21 + RSI range + volume surge + optional VWAP
    "macd"          — Pure MACD crossover + volume surge
    "exhaustion"    — %R Exhaustion (red zone) + CM RSI < threshold +
                      volume trending up + MACD bull crossover (default)
    """
    df = df.copy()
    strategy = cfg.get("strategy", "exhaustion")

    # ── Common base indicators (used by all strategies) ──────
    df["ema_short"]  = ema(df["close"], cfg.get("ema_short", 8))
    df["ema_long"]   = ema(df["close"], cfg.get("ema_long",  21))
    df["vol_avg"]    = df["volume"].rolling(20).mean()
    df["vol_surge"]  = df["volume"] > (df["vol_avg"] * cfg.get("volume_surge_mult", 1.5))
    df["vwap"]       = vwap_calc(df)
    df["above_vwap"] = df["close"] > df["vwap"]
    df["cross_up"]   = (df["ema_short"] >  df["ema_long"]) & (df["ema_short"].shift(1) <= df["ema_long"].shift(1))
    df["cross_down"] = (df["ema_short"] <  df["ema_long"]) & (df["ema_short"].shift(1) >= df["ema_long"].shift(1))
    df["signal"]     = "HOLD"

    # ── EMA Crossover strategy ───────────────────────────────
    if strategy == "ema_crossover":
        df["rsi"]   = rsi(df["close"], cfg.get("rsi_period", 14))
        rsi_lo      = cfg.get("rsi_min_buy",    45)
        rsi_hi      = cfg.get("rsi_overbought", 65)
        rsi_sel     = cfg.get("rsi_sell",       75)
        buy_cond    = df["cross_up"] & (df["rsi"] >= rsi_lo) & (df["rsi"] < rsi_hi) & df["vol_surge"]
        if cfg.get("use_vwap", True):
            buy_cond = buy_cond & df["above_vwap"]
        df.loc[buy_cond,                                  "signal"] = "BUY"
        df.loc[df["cross_down"] | (df["rsi"] > rsi_sel), "signal"] = "SELL"

    # ── MACD strategy ────────────────────────────────────────
    elif strategy == "macd":
        df = compute_macd(df, cfg)
        buy_cond  = df["macd_bull"] & df["vol_surge"]
        sell_cond = df["macd_bear"]
        df.loc[buy_cond,  "signal"] = "BUY"
        df.loc[sell_cond, "signal"] = "SELL"

    # ── Exhaustion strategy (default) ────────────────────────
    else:
        df = compute_percent_r_exhaustion(df, cfg)
        df = compute_rmi(df, cfg)
        df = compute_obv_oscillator(df, cfg)
        df = compute_volume_trending_up(df, cfg)
        df = compute_macd(df, cfg)

        min_boxes   = cfg.get("rte_min_boxes",       1)
        entry_win   = cfg.get("rte_entry_window",     3)   # bars after reversal still valid
        min_support = cfg.get("rte_min_supporting",   1)   # how many of [rmi, vol, macd] required

        # ── Primary gate: reversal happened within the last entry_win bars ──
        # A reversal on bar 0, 1, or 2 ago is still actionable — you don't need
        # to catch the exact reversal bar to ride the move.
        rte_valid = df["rte_reversal"] if entry_win <= 1 else (
            df["rte_reversal"]
              .rolling(window=entry_win, min_periods=1)
              .max()
              .fillna(0)
              .astype(bool)
        )
        # Require minimum consecutive boxes in the current episode (streak)
        primary = rte_valid & (df["rte_boxes_streak"] >= min_boxes)

        # ── Supporting conditions — scored, not AND-gated ────────────────────
        # rmi_signal : CM RSI-2 bullish (close > SMA200, < SMA5, RSI-2 < threshold)
        # volume     : vol_trend_up OR vol_surge — either MA trend or bar spike counts
        # macd       : MACD line above signal line (persistent momentum)
        support_scores = pd.Series(0, index=df.index)
        if cfg.get("use_rmi", True):
            support_scores += df["rmi_signal"].astype(int)
        if cfg.get("use_volume_trending_up", True):
            vol_ok = df["vol_trend_up"] | df["vol_surge"]
            support_scores += vol_ok.astype(int)
        if cfg.get("use_macd", True):
            # MACD line above signal = bullish momentum (not just the crossover bar)
            macd_pos = df["macd_line"] > df["macd_signal_line"]
            support_scores += macd_pos.astype(int)

        # ── Combined: primary event + minimum supporting confirmations ───────
        if cfg.get("use_rte_exhaustion", True):
            combined_buy = primary & (support_scores >= min_support)
        else:
            # Exhaustion disabled — fall back to plain EMA crossover
            combined_buy = df["cross_up"] & df["vol_surge"]

        df.loc[combined_buy, "signal"] = "BUY"

        # ── Exits — EMA cross + MACD bear ────────────────────────────────────
        # rte_extreme (price back in overbought zone) is NOT included here because
        # compute_signals() has no concept of "in position" — adding it would mark
        # every overbought stock as SELL in the log, even ones we don't hold.
        # The rte_extreme re-entry exit is checked directly in the scan loop,
        # where in_pos is known, and logged as [EXHAUSTION-REENTRY].
        sell_conds = [df["cross_down"]]
        if cfg.get("use_macd", True):
            sell_conds.append(df["macd_bear"])

        combined_sell = sell_conds[0]
        for c in sell_conds[1:]:
            combined_sell = combined_sell | c
        df.loc[combined_sell, "signal"] = "SELL"

    # Ensure rsi column always exists (used by pyramid check)
    if "rsi" not in df.columns:
        df["rsi"] = rsi(df["close"], cfg.get("rsi_period", 14))

    # ── ATR — always computed; used for ATR-based position sizing ────────────
    # True Range = max(H-L, |H-prevC|, |L-prevC|)
    _atr_p = cfg.get("atr_period", 14)
    _tr_hl  = df["high"] - df["low"]
    _tr_hc  = (df["high"] - df["close"].shift(1)).abs()
    _tr_lc  = (df["low"]  - df["close"].shift(1)).abs()
    df["atr"] = pd.concat([_tr_hl, _tr_hc, _tr_lc], axis=1).max(axis=1).ewm(
        span=_atr_p, adjust=False).mean()

    return df


def calc_rvol(df: pd.DataFrame, avg_daily_vol: int = 0) -> float:
    """
    Calculate RVOL using bar data only — no yfinance required.
    Compares today's accumulated volume to the average volume over the same
    number of bars on previous trading days in the same DataFrame.

    Falls back to time-fraction method if yfinance avg_daily_vol is supplied
    and bar history is insufficient.
    """
    try:
        now_et = datetime.now(ET)
        today  = now_et.date()

        # Split df into per-day groups
        dates_series = pd.Series([i.date() for i in df.index], index=df.index)
        today_mask   = dates_series == today
        today_bars   = df.loc[today_mask]
        today_vol    = float(today_bars["volume"].sum())

        if today_vol == 0:
            return 0.0

        n_today = len(today_bars)   # how many bars have printed so far today

        # Collect same-length volume windows from each previous day
        prev_dates = sorted(d for d in dates_series.unique() if d < today)
        prev_vols  = []
        for d in prev_dates:
            d_bars = df.loc[dates_series == d]
            if len(d_bars) == 0:
                continue
            # Use the first n_today bars of each prior day (same time window)
            window = d_bars.iloc[:n_today]["volume"].sum()
            if window > 0:
                prev_vols.append(float(window))

        if prev_vols:
            avg_same_window = sum(prev_vols) / len(prev_vols)
            return round(today_vol / avg_same_window, 2) if avg_same_window > 0 else 0.0

        # Fallback: time-fraction method using yfinance avg if available
        if avg_daily_vol > 0:
            market_open   = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            elapsed_mins  = max(1.0, (now_et - market_open).total_seconds() / 60.0)
            time_fraction = min(1.0, elapsed_mins / 390.0)
            expected      = avg_daily_vol * time_fraction
            return round(today_vol / expected, 2) if expected > 0 else 0.0

        return 0.0
    except Exception:
        return 0.0

# ═══════════════════════════════════════════════════════════
#  ALPACA HELPERS
# ═══════════════════════════════════════════════════════════

def connect_alpaca(cfg: dict):
    from alpaca.trading.client import TradingClient
    from alpaca.data.historical import StockHistoricalDataClient
    paper   = cfg.get("paper", True)
    trading = TradingClient(cfg["api_key"], cfg["secret_key"], paper=paper)
    data    = StockHistoricalDataClient(cfg["api_key"], cfg["secret_key"])
    return trading, data

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

def _get_feed_arg(cfg: dict = None) -> dict:
    """Return the correct Alpaca DataFeed kwarg based on config.
    Defaults to IEX (free tier). Set data_feed='SIP' in config when subscribed
    to Alpaca Unlimited for full pre-market data and tighter spreads."""
    try:
        from alpaca.data.enums import DataFeed as _DF
        cfg = cfg or STATE.config
        feed_name = cfg.get("data_feed", "IEX").upper()
        feed = _DF.SIP if feed_name == "SIP" else _DF.IEX
        return {"feed": feed}
    except Exception:
        return {}

def get_live_price(data_client, ticker: str, cfg: dict = None) -> float | None:
    """Return latest close using StockLatestBarRequest — single-bar, fast."""
    try:
        from alpaca.data.requests import StockLatestBarRequest
        resp = data_client.get_stock_latest_bar(StockLatestBarRequest(
            symbol_or_symbols=ticker, **_get_feed_arg(cfg)))
        bar  = resp.get(ticker)
        return float(bar.close) if bar else None
    except Exception:
        return None

def fetch_bars(data_client, ticker: str, cfg: dict) -> pd.DataFrame | None:
    try:
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        tf_map = {
            "1Min":  TimeFrame(1,  TimeFrameUnit.Minute),
            "5Min":  TimeFrame(5,  TimeFrameUnit.Minute),
            "15Min": TimeFrame(15, TimeFrameUnit.Minute),
            "1Hour": TimeFrame(1,  TimeFrameUnit.Hour),
            "1Day":  TimeFrame(1,  TimeFrameUnit.Day),
        }
        tf  = tf_map.get(cfg.get("bar_timeframe","5Min"), TimeFrame(5, TimeFrameUnit.Minute))
        try:
            from alpaca.data.enums import DataFeed as _DF
            _feed_arg = {"feed": _DF.IEX}
        except Exception:
            _feed_arg = {}
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=tf,
            start=datetime.now(timezone.utc) - timedelta(days=10),
            limit=cfg.get("bar_count", 300),
            **_feed_arg,
        )
        bars = data_client.get_stock_bars(req).df
        if bars is None or bars.empty: return None
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(ticker, level="symbol")
        bars = bars[["open","high","low","close","volume"]].copy()
        bars["close"] = pd.to_numeric(bars["close"], errors="coerce")
        return bars.dropna(subset=["close"])
    except Exception as e:
        dlog.error(f"fetch_bars {ticker}: {e}")
        return None

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
    try:
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums    import OrderSide, TimeInForce
        positions = {p.symbol: int(float(p.qty)) for p in trading_client.get_all_positions()}
        qty = positions.get(ticker, 0)
        if qty < 1:
            return False
        trading_client.submit_order(MarketOrderRequest(
            symbol=ticker, qty=qty,
            side=OrderSide.SELL, time_in_force=TimeInForce.DAY,
        ))
        return True
    except Exception as e:
        dlog.error(f"close_position {ticker}: {e}")
        return False

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

        # ── Fetch + compute all watched tickers in parallel ──────────────
        def _fast_job(args):
            _t, _dc, _fc = args
            _df = fetch_bars(_dc, _t, _fc)
            if _df is None or len(_df) < fast_min:
                return _t, None
            try:
                return _t, compute_signals(_df, _fc)
            except:
                return _t, None

        _futs = {_FETCH_EXECUTOR.submit(_fast_job, (t, data_client, fast_cfg)): t
                 for t in watch_copy}
        _done_f, _ = _cf_module.wait(_futs.keys(), timeout=8)
        fast_results: dict = {}
        for _f in _done_f:
            _t = _futs[_f]
            try:
                _, _df_sig = _f.result()
                fast_results[_t] = _df_sig
            except:
                fast_results[_t] = None

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
            # Fetch live prices in parallel for all open positions
            def _price_job(t):
                p = get_live_price(_dclient, t, cfg)
                return t, p
            _pfuts = {_FETCH_EXECUTOR.submit(_price_job, t): t for t in _ep_copy}
            _done_p, _ = _cf_module.wait(_pfuts.keys(), timeout=6)
            _live_prices = {}
            for _f in _done_p:
                _t = _pfuts[_f]
                try:
                    _, _p = _f.result()
                    if _p:
                        _live_prices[_t] = _p
                except:
                    pass

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
                if cfg.get("use_rvol") and cfg.get("min_rvol", 0) > 0:
                    avg_vol = si.get("avg_vol", 0)   # yfinance fallback, may be 0
                    rvol = calc_rvol(df, avg_vol)
                    if rvol > 0 and rvol < cfg["min_rvol"]:
                        dlog.info(f"  {ticker:<6} SKIP — rvol {rvol:.2f} < min {cfg['min_rvol']}")
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
                    if cfg.get("use_rvol", True):
                        _min_rvol    = cfg.get("min_rvol", 2.0)
                        _entry_rvol  = calc_rvol(df)
                        if _entry_rvol > 0 and _entry_rvol < _min_rvol:
                            dlog.info(f"  [RVOL]  {ticker} BUY skipped — entry RVOL {_entry_rvol:.2f}x < min {_min_rvol:.1f}x (volume faded)")
                            continue
                        elif cfg.get("debug_signals") and _entry_rvol > 0:
                            dlog.info(f"  [RVOL]  {ticker} entry RVOL {_entry_rvol:.2f}x ✓")

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
                        bool(_latest.get("rmi_signal",    False)),
                        bool(_latest.get("vol_trend_up",  False)),
                        bool(_latest.get("macd_bull",     False)),
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
#  STOCKTWITS TRENDING  (sentiment page scraper)
#  The old /api/2/trending/symbols.json endpoint locked down in 2024.
#  We now scrape stocktwits.com/sentiment instead — no auth needed.
# ═══════════════════════════════════════════════════════════

_ST_SENTIMENT_URL = "https://stocktwits.com/sentiment"
_ST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _walk_json_for_tickers(obj, depth: int = 0, max_depth: int = 7) -> list:
    """Recursively extract stock ticker values from a nested JSON object."""
    if depth > max_depth:
        return []
    out = []
    if isinstance(obj, dict):
        for key in ("symbol", "ticker", "sym"):
            val = obj.get(key, "")
            if isinstance(val, str):
                sym = val.strip().upper()
                if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                    out.append(sym)
        for v in obj.values():
            out.extend(_walk_json_for_tickers(v, depth + 1, max_depth))
    elif isinstance(obj, list):
        for item in obj[:200]:
            out.extend(_walk_json_for_tickers(item, depth + 1, max_depth))
    seen: set = set()
    return [t for t in out if not (t in seen or seen.add(t))]


def fetch_stocktwits_trending() -> list:
    """
    Pull trending tickers from the StockTwits sentiment page.

    Strategy 1 — __NEXT_DATA__ JSON blob (Next.js): fastest and most reliable.
    Strategy 2 — BeautifulSoup link scan: fallback if Next.js payload changes.
    Both strategies filter to plain 2-5 char alpha US equity symbols only.
    """
    try:
        import requests
        from bs4 import BeautifulSoup
    except ImportError as ie:
        dlog.warning(f"[TREND] StockTwits scraper requires 'requests' + 'beautifulsoup4': {ie}")
        return []

    try:
        resp = requests.get(_ST_SENTIMENT_URL, headers=_ST_HEADERS, timeout=12)
        if resp.status_code != 200:
            dlog.debug(f"[TREND] StockTwits unavailable (HTTP {resp.status_code}) — using Finviz only")
            return []

        soup    = BeautifulSoup(resp.text, "html.parser")
        tickers: list = []

        # ── Strategy 1: __NEXT_DATA__ JSON (Next.js embed) ────────────────
        nd_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        if nd_tag and nd_tag.string:
            try:
                nd    = json.loads(nd_tag.string)
                props = nd.get("props", {}).get("pageProps", {})
                for key in ("trendingSymbols", "trending", "symbols",
                            "rankings", "data", "sentimentData"):
                    items = props.get(key, [])
                    if isinstance(items, list) and items:
                        for item in items:
                            sym = str(
                                item.get("symbol") or item.get("ticker") or ""
                            ).strip().upper()
                            if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                                tickers.append(sym)
                        if tickers:
                            break
                if not tickers:
                    tickers = _walk_json_for_tickers(nd)
            except Exception as je:
                dlog.debug(f"[TREND] ST __NEXT_DATA__ parse error: {je}")

        # ── Strategy 2: BeautifulSoup — /symbol/TICKER hrefs ──────────────
        if not tickers:
            seen: set = set()
            for a in soup.find_all("a", href=True):
                href = str(a["href"])
                if "/symbol/" in href:
                    sym = href.split("/symbol/")[-1].split("?")[0].split("/")[0].upper()
                    if sym and sym.isalpha() and 2 <= len(sym) <= 5 and sym not in seen:
                        tickers.append(sym)
                        seen.add(sym)

        if not tickers:
            dlog.warning("[TREND] StockTwits: no tickers found — page structure may have changed")
            return []

        dlog.debug(f"[TREND] StockTwits raw ({len(tickers)} equities): {', '.join(tickers[:20])}")
        return tickers

    except Exception as e:
        dlog.error(f"[TREND] StockTwits sentiment fetch failed: {e}")
        return []


# ── kept for reference — replaced by fetch_stocktwits_trending() above ──
def _fetch_yahoo_trending_UNUSED() -> list:
    """
    Pull trending US tickers from Yahoo Finance (no API key required).

    Endpoint: /v1/finance/trending/US — returns the tickers users are actively
    viewing on Yahoo Finance right now, a good proxy for social momentum.
    Falls back to the secondary query server if the first is unavailable.
    """
    for url in _YAHOO_TRENDING_URLS:
        try:
            req = urllib.request.Request(url, headers=_YAHOO_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    continue
                data = json.loads(resp.read().decode())
            result = data.get("finance", {}).get("result", [])
            if not result:
                dlog.warning("[TREND] Yahoo trending: empty result payload")
                continue
            quotes  = result[0].get("quotes", [])
            tickers = []
            for q in quotes:
                sym = str(q.get("symbol", "")).strip().upper()
                # Plain US equity symbols only — skip crypto (BTC-USD), ETFs, warrants
                if sym and sym.isalpha() and 2 <= len(sym) <= 5:
                    tickers.append(sym)
            dlog.debug(f"[TREND] Yahoo raw ({len(tickers)} equities): {', '.join(tickers[:20])}")
            return tickers
        except Exception as e:
            dlog.debug(f"[TREND] Yahoo trending error ({url}): {e}")
            continue
    dlog.error("[TREND] Yahoo trending fetch failed — both endpoints unreachable")
    return []


def fetch_finviz_trending() -> list:
    """
    Pull momentum tickers from the Finviz screener using finvizfinance.

    Filters are driven by config keys:
        finviz_exchange    — "NASDAQ", "NYSE", "AMEX", or "" for all
        finviz_performance — Finviz Performance label, e.g. "Week Up"
        finviz_rel_volume  — Relative Volume label, e.g. "Over 2"
        finviz_avg_volume  — Average Volume label, e.g. "Over 500K"
        finviz_float       — Float label, e.g. "Under 20M"
        finviz_signal      — reserved / not supported by finvizfinance (keep "")
        trending_max_price — upper price cap (shared with StockTwits + Finviz filter)
        finviz_include_news — if True, log top Finviz market news headlines

    Requires:  pip install finvizfinance
    """
    try:
        from finvizfinance.screener.overview import Overview
    except ImportError:
        dlog.warning("[TREND] finvizfinance not installed — skipping Finviz source  "
                     "(pip install finvizfinance)")
        return []

    cfg = STATE.config

    try:
        from finvizfinance.screener.overview import Overview
        fov = Overview()

        max_price   = cfg.get("trending_max_price", 5.0)
        exchange    = cfg.get("finviz_exchange",    "NASDAQ")
        performance = cfg.get("finviz_performance", "Week Up")
        rel_volume  = cfg.get("finviz_rel_volume",  "Over 2")
        avg_volume  = cfg.get("finviz_avg_volume",  "Over 500K")
        float_size  = cfg.get("finviz_float",       "Under 20M")
        signal      = cfg.get("finviz_signal",      "")   # not supported by finvizfinance

        # ── Build filter dict ─────────────────────────────────────────────
        # Standard low-float momentum screen:
        #   Exchange   — focus on NASDAQ runners (most liquid small-caps)
        #   Price      — capped at config value (e.g. <$10)
        #   Float      — small float amplifies % moves on volume spikes
        #   Avg Volume — baseline liquidity so fills aren't terrible
        #   Rel Volume — today's volume vs average; the real momentum trigger
        #   Performance — already moving this week
        #   Signal     — built-in catalyst filter (Major News, Unusual Volume, etc.)
        price_str = f"Under ${int(max_price)}" if max_price == int(max_price) else f"Under ${max_price}"
        filters: dict = {
            "Price":           price_str,
            "Relative Volume": rel_volume,
            "Performance":     performance,
        }
        if exchange:    filters["Exchange"]       = exchange
        if float_size:  filters["Float"]          = float_size
        if avg_volume:  filters["Average Volume"] = avg_volume
        if signal:      filters["Signal"]         = signal

        dlog.debug(f"[TREND] Finviz screener filters: {filters}")
        fov.set_filter(filters_dict=filters)
        df = fov.screener_view()

        if df is None or df.empty:
            dlog.warning("[TREND] Finviz screener returned no results")
            return []

        # Normalise column name — finvizfinance uses 'Ticker' in recent versions
        ticker_col = next((c for c in df.columns if c.lower() == "ticker"), None)
        if ticker_col is None:
            dlog.warning(f"[TREND] Finviz screener: no Ticker column found (cols={list(df.columns)})")
            return []

        tickers = [
            str(t).strip().upper()
            for t in df[ticker_col].dropna().tolist()
            if str(t).strip().isalpha() and 2 <= len(str(t).strip()) <= 5
        ]

        dlog.debug(f"[TREND] Finviz raw ({len(tickers)} equities): {', '.join(tickers[:25])}")

        # ── Optional: log top market headlines for context ─────────────────
        if cfg.get("finviz_include_news", True):
            try:
                from finvizfinance.news import News
                news_obj = News()
                all_news = news_obj.get_news()
                headlines = []
                if isinstance(all_news, dict):
                    for key in ("news", "blogs"):
                        ndf2 = all_news.get(key)
                        if ndf2 is not None and not ndf2.empty:
                            title_col = next((c for c in ndf2.columns if "title" in c.lower()), None)
                            if title_col:
                                headlines += ndf2[title_col].head(3).tolist()
                if headlines:
                    dlog.debug("[TREND] Finviz headlines: " +
                               "  |  ".join(str(h)[:90] for h in headlines[:5]))
            except Exception as ne:
                dlog.debug(f"[TREND] Finviz news fetch skipped: {ne}")

        return tickers

    except Exception as e:
        dlog.error(f"[TREND] Finviz fetch failed: {e}")
        return []


def filter_under_five(tickers: list, data_client, max_price: float = None) -> list:
    """Return only tickers whose latest ask/bid price is at or under max_price."""
    if not tickers or data_client is None:
        return tickers
    if max_price is None:
        max_price = STATE.config.get("trending_max_price", 5.0)
    try:
        from alpaca.data.requests import StockLatestQuoteRequest
        req    = StockLatestQuoteRequest(symbol_or_symbols=tickers, **_get_feed_arg())
        quotes = data_client.get_stock_latest_quote(req)
        result    = []
        filtered  = []
        price_map = {}
        for t in tickers:
            q = quotes.get(t)
            if q:
                price = float(q.ask_price or q.bid_price or 0)
                if 0 < price < max_price:   # strictly under — e.g. 5.0 means <$5.00
                    result.append(t)
                    price_map[t] = round(price, 4)
                else:
                    filtered.append(f"{t}(${price:.2f})")
            else:
                filtered.append(f"{t}(no quote)")
        if filtered:
            dlog.debug(f"[TREND] Filtered out (>${max_price}): {', '.join(filtered)}")
        # Persist prices in state for UI tooltip display
        with STATE.lock:
            STATE.trending_prices = price_map
        return result
    except Exception as e:
        # Unauthorized means data client not ready yet — silent fallback, not an error
        err_str = str(e)
        if "401" in err_str or "unauthorized" in err_str.lower():
            dlog.debug(f"[TREND] Price filter skipped — data client not ready")
        else:
            dlog.warning(f"[TREND] Price filter failed: {e}")
        return tickers   # fallback: return unfiltered


def update_trending_watchlist() -> list:
    """
    Fetch trending stocks from StockTwits + Finviz (if enabled),
    merge/deduplicate, filter by trending_max_price, then update STATE.trending_tickers.
    Called on startup and every 5 minutes by trending_updater().

    Source priority: StockTwits first (social momentum), then Finviz extras appended.
    """
    cfg = STATE.config

    # ── Fetch from each source ─────────────────────────────────────────
    st_raw = fetch_stocktwits_trending()
    fv_raw = fetch_finviz_trending() if cfg.get("finviz_enabled", True) else []

    # ── Merge — StockTwits first, then Finviz additions, deduplicated ──
    # Build sources map while merging so each ticker knows its origin.
    st_set = set(st_raw)
    fv_set = set(fv_raw)
    raw_sources: dict = {}
    seen: set = set()
    raw:  list = []
    for t in st_raw + fv_raw:
        if t not in seen:
            seen.add(t)
            raw.append(t)
        # Determine source label (may upgrade to "both" if in both lists)
        if t in st_set and t in fv_set:
            raw_sources[t] = "both"
        elif t in st_set:
            raw_sources[t] = "stocktwits"
        else:
            raw_sources[t] = "finviz"

    fv_new = len([t for t in fv_raw if t not in st_set])

    if not raw:
        dlog.warning("[TREND] No tickers returned from any source — watchlist unchanged")
        with STATE.lock:
            return list(STATE.trending_tickers)

    # ── Price filter via Alpaca quotes ─────────────────────────────────
    dc = STATE.data_client
    if dc is None:
        try:
            if cfg.get("api_key") and "YOUR_" not in cfg["api_key"]:
                _, dc = connect_alpaca(cfg)
        except Exception:
            pass

    max_p    = cfg.get("trending_max_price", 5.0)
    filtered = filter_under_five(raw, dc, max_price=max_p) if dc else raw

    if filtered:
        ts = datetime.now(ET).strftime("%H:%M:%S ET")
        # Keep only the sources for tickers that survived the price filter
        filtered_sources = {t: raw_sources.get(t, "unknown") for t in filtered}
        with STATE.lock:
            # Never overwrite the user's static watchlist — trending suggestions
            # are kept separately and merged by the scan loop.
            STATE.trending_tickers = filtered
            STATE.trending_sources = filtered_sources
            STATE.trending_updated = ts
        # Single-line summary: counts by source + ticker list
        n_st   = sum(1 for s in filtered_sources.values() if s == "stocktwits")
        n_fv   = sum(1 for s in filtered_sources.values() if s == "finviz")
        n_both = sum(1 for s in filtered_sources.values() if s == "both")
        src_str = "  ".join(filter(None, [
            f"{n_st}ST"   if n_st   else "",
            f"{n_fv}FV"   if n_fv   else "",
            f"{n_both}✦"  if n_both else "",
        ]))
        dlog.info(f"[TREND] {len(filtered)} tickers ({src_str}) — {', '.join(filtered)}")
    return filtered


def trending_updater():
    """
    Background thread — refresh trending watchlist on a market-hours schedule.

    Schedule:
      • First fetch at 9:15 AM ET (or immediately if already past 9:15 during market hours)
      • Then every 5 minutes aligned to the clock: 9:20, 9:25, 9:30 … 15:55
      • No fetches outside 9:15 AM–4:00 PM ET, or on weekends
    """
    dlog.info("[TREND] Trending updater started — waiting for market hours (9:15 AM ET)")

    while True:
        now  = datetime.now(ET)
        wday = now.weekday()   # 0=Mon … 6=Sun

        # ── Weekend — sleep until Monday ────────────────────────────
        if wday >= 5:
            # seconds until next Monday 9:15 AM
            days_ahead = 7 - wday   # 5→2, 6→1
            next_monday = (now + timedelta(days=days_ahead)).replace(
                hour=9, minute=15, second=0, microsecond=0)
            wait = (next_monday - now).total_seconds()
            dlog.info(f"[TREND] Weekend — next fetch {next_monday.strftime('%a %m/%d %H:%M ET')}")
            time.sleep(max(wait, 60))
            continue

        # ── Weekday boundaries ───────────────────────────────────────
        session_start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
        session_end   = now.replace(hour=16, minute=0,  second=0, microsecond=0)

        if now < session_start:
            wait = (session_start - now).total_seconds()
            dlog.info(f"[TREND] Pre-market — first fetch at 9:15 AM ET ({wait/60:.0f} min away)")
            time.sleep(min(wait, 60))   # wake every minute to re-check date/config
            continue

        if now >= session_end:
            # After hours — sleep until next trading day 9:15 AM
            if wday == 4:   # Friday → Monday
                days_ahead = 3
            else:
                days_ahead = 1
            next_open = (now + timedelta(days=days_ahead)).replace(
                hour=9, minute=15, second=0, microsecond=0)
            wait = (next_open - now).total_seconds()
            dlog.info(f"[TREND] Market closed — next fetch {next_open.strftime('%a %m/%d %H:%M ET')}")
            time.sleep(min(wait, 300))
            continue

        # ── In session — fetch now ───────────────────────────────────
        try:
            update_trending_watchlist()
        except Exception as e:
            dlog.error(f"[TREND] Updater error: {e}")

        # Sleep until the next 5-minute clock boundary (9:20, 9:25, 9:30…)
        now         = datetime.now(ET)
        mins_past   = now.minute % 5
        secs_to_next = (5 - mins_past) * 60 - now.second
        if secs_to_next <= 5:          # avoid a near-zero sleep causing a double-fire
            secs_to_next += 300
        next_fire = now + timedelta(seconds=secs_to_next)
        dlog.info(f"[TREND] Next trending fetch at {next_fire.strftime('%H:%M:%S ET')}")
        time.sleep(secs_to_next)


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
    dlog.info("[DASH] Dashboard ready — trending updater + auto-scheduler started")
    # Open browser automatically (slight delay so the server is fully bound first)
    def _open_browser():
        import time as _t, webbrowser
        _t.sleep(1.5)
        webbrowser.open(f"http://localhost:{PORT}")
    threading.Thread(target=_open_browser, daemon=True).start()

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML.replace("{{BOT_VERSION}}", BOT_VERSION))

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

@app.post("/api/config")
async def api_config(request: Request):
    body = await request.json()
    with STATE.lock:
        # Update allowed keys only
        safe_keys = [
            "paper","api_key","secret_key","tickers",
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
    """
    cfg = dict(STATE.config)
    dc  = STATE.data_client
    if dc is None:
        return JSONResponse({"ok": False, "error": "Bot not connected — hit Test Connection first"}, status_code=503)

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
        return JSONResponse({"watching": {t: dict(v) for t, v in STATE.watching.items()}})

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
#  DASHBOARD HTML
# ═══════════════════════════════════════════════════════════

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Alpaca Momentum Bot</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0d1117;--card:#161b22;--card2:#1c2128;--border:#30363d;
  --accent:#58a6ff;--green:#3fb950;--red:#f85149;--yellow:#e3b341;
  --purple:#bc8cff;--text:#c9d1d9;--muted:#8b949e;
  --font:\'Segoe UI\',system-ui,sans-serif;--mono:\'Cascadia Code\',\'Consolas\',monospace;
}
/* === FULL-VIEWPORT LAYOUT === */
html,body{height:100%;overflow:hidden}
body{
  display:flex;flex-direction:column;
  background:var(--bg);color:var(--text);
  font-family:var(--font);font-size:14px;
}
/* === HEADER === */
header{
  display:flex;align-items:stretch;
  background:var(--card);border-bottom:1px solid var(--border);
  flex-shrink:0;z-index:200;height:52px;
}
.hdr-logo{
  padding:0 20px;font-size:15px;font-weight:700;color:#fff;
  letter-spacing:-.3px;white-space:nowrap;border-right:1px solid var(--border);
  display:flex;align-items:center;gap:6px;
}
.hdr-logo .ac{color:var(--accent)}
.hdr-logo .ver{font-size:10px;font-weight:400;color:var(--muted);margin-left:2px}
.hdr-status{
  padding:0 16px;display:flex;align-items:center;gap:8px;
  border-right:1px solid var(--border);white-space:nowrap;flex-shrink:0;
}
.hdr-stats{display:flex;align-items:stretch;flex:1;overflow:hidden}
.hdr-stat{
  display:flex;flex-direction:column;justify-content:center;
  padding:0 16px;border-right:1px solid var(--border);white-space:nowrap;
}
.hdr-stat-lbl{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:2px}
.hdr-stat-val{font-size:14px;font-weight:700;font-family:var(--mono);color:#fff;line-height:1}
.hdr-stat-val.pos{color:var(--green)}
.hdr-stat-val.neg{color:var(--red)}
.hdr-stat-sub{font-size:10px;color:var(--muted);margin-top:1px}
.hdr-controls{padding:0 16px;display:flex;align-items:center;gap:8px;flex-shrink:0}
/* === CANVAS === */
.canvas{
  flex:1;display:grid;
  grid-template-columns:260px 1fr 380px;
  overflow:hidden;min-height:0;
}
/* === LEFT COLUMN === */
.col-left{
  display:flex;flex-direction:column;
  border-right:1px solid var(--border);overflow:hidden;
}
.tab-strip{
  display:flex;border-bottom:1px solid var(--border);
  flex-shrink:0;background:var(--card2);
}
.tab-btn{
  flex:1;padding:10px 0;font-size:11px;font-weight:700;
  letter-spacing:.5px;text-transform:uppercase;cursor:pointer;
  border:none;background:transparent;color:var(--muted);
  border-bottom:2px solid transparent;transition:.15s;
}
.tab-btn.active{color:#fff;border-bottom-color:var(--accent)}
.tab-btn:hover:not(.active){color:var(--text)}
.tab-pane{display:none;flex-direction:column;overflow-y:auto;flex:1;min-height:0}
.tab-pane.active{display:flex}
.tab-pane::-webkit-scrollbar{width:4px}
.tab-pane::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
/* ── Analytics tab ───────────────────────────────────────── */
.an-toolbar{display:flex;align-items:center;padding:8px 10px;border-bottom:1px solid var(--border);flex-shrink:0}
.an-cards{display:flex;flex-wrap:wrap;gap:8px;padding:10px;flex-shrink:0}
.an-card{background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:10px 14px;min-width:90px;flex:1}
.an-card-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;margin-bottom:3px}
.an-card-value{font-size:18px;font-weight:700;color:var(--text)}
.an-empty{color:var(--muted);font-size:12px;padding:16px 10px}
.an-table{width:100%;border-collapse:collapse;font-size:11px;margin:0}
.an-table th{color:var(--muted);font-weight:600;text-transform:uppercase;font-size:10px;padding:4px 8px;border-bottom:1px solid var(--border);text-align:left}
.an-table td{padding:4px 8px;border-bottom:1px solid rgba(48,54,61,.5)}
#an-chart-wrap{padding:10px;flex-shrink:0}
#an-ticker-wrap,#an-trades-wrap{padding:0 10px 10px;flex-shrink:0}
/* Watchlist */
.wl-toolbar{
  display:flex;gap:5px;padding:10px;flex-wrap:wrap;
  border-bottom:1px solid var(--border);flex-shrink:0;align-items:center;
}
.wl-toolbar input{
  background:var(--card2);border:1px solid var(--border);border-radius:6px;
  color:var(--text);padding:5px 8px;font-size:12px;font-family:var(--mono);
}
.wl-toolbar input:focus{outline:none;border-color:var(--accent)}
#wl-add-input{width:85px}
#wl-search{width:95px}
.wl-chips{
  display:flex;flex-wrap:wrap;gap:5px;
  padding:10px;overflow-y:auto;flex:1;min-height:0;align-content:flex-start;
}
.wl-chips::-webkit-scrollbar{width:4px}
.wl-chips::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.wl-chip{
  display:inline-flex;align-items:center;gap:4px;
  background:#1f3a5f;border:1px solid var(--accent);color:var(--accent);
  padding:3px 5px 3px 9px;border-radius:12px;font-size:11px;
  font-family:var(--mono);font-weight:600;
}
.wl-chip.editing{border-color:var(--yellow);background:#2a2000}
.wl-chip .chip-lbl{cursor:default}
.wl-chip .chip-tv{color:var(--accent);text-decoration:none;cursor:pointer}
.wl-chip .chip-tv:hover{color:var(--green)}
.wl-chip .chip-edit-inp{
  width:70px;background:transparent;border:none;outline:none;
  color:var(--yellow);font-size:11px;font-family:var(--mono);font-weight:600;
}
.wl-chip .ic{cursor:pointer;font-size:12px;padding:0 2px;border-radius:4px;color:var(--muted)}
.wl-chip .ic:hover{color:#fff}
.wl-chip .ic.rm:hover{color:var(--red)}
.wl-chip.hidden{display:none}
#wl-paste-area{
  width:calc(100% - 20px);height:55px;resize:none;
  background:var(--card2);border:1px solid var(--border);border-radius:6px;
  color:var(--text);padding:6px 8px;font-size:11px;font-family:var(--mono);
  margin:0 10px 8px;flex-shrink:0;
}
#wl-paste-area:focus{outline:none;border-color:var(--accent)}
.wl-footer{
  padding:8px 10px;border-top:1px solid var(--border);
  display:flex;align-items:center;gap:6px;flex-wrap:wrap;
  background:var(--card2);flex-shrink:0;
}
/* Locked tickers */
.locked-bar{
  border-top:1px solid var(--border);padding:8px 10px;
  background:#140f00;flex-shrink:0;
}
.locked-bar .locked-lbl{
  font-size:9px;text-transform:uppercase;letter-spacing:.5px;
  color:var(--yellow);margin-bottom:5px;
}
.lock-chip{
  display:inline-flex;align-items:center;gap:4px;
  background:#2a1f00;border:1px solid var(--yellow);
  border-radius:10px;font-size:11px;font-family:var(--mono);font-weight:700;
  color:var(--yellow);padding:2px 8px;cursor:pointer;transition:.15s;margin:2px;
}
.lock-chip:hover{background:#3d2e00;border-color:#fff;color:#fff}
/* Trending tab */
.trend-tab-header{
  display:flex;align-items:center;gap:8px;padding:9px 12px;
  border-bottom:1px solid var(--border);background:var(--card2);flex-shrink:0;
}
#trend-chips{
  display:flex;flex-wrap:wrap;gap:6px;padding:12px;
  overflow-y:auto;flex:1;min-height:0;align-content:flex-start;
}
#trend-chips::-webkit-scrollbar{width:4px}
#trend-chips::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
/* === CENTER COLUMN === */
.col-center{
  display:flex;flex-direction:column;overflow:hidden;
  border-right:1px solid var(--border);
}
.card{
  display:flex;flex-direction:column;overflow:hidden;
  border-bottom:1px solid var(--border);
}
.card.positions{flex:1.4;min-height:0}
.card.trades{flex:1;min-height:0}
.card-header{
  display:flex;align-items:center;gap:8px;padding:10px 14px;
  border-bottom:1px solid var(--border);flex-shrink:0;
  background:var(--card2);
}
.card-title{font-weight:700;font-size:13px;color:#fff}
.count-badge{
  background:var(--bg);color:var(--muted);
  padding:1px 7px;border-radius:8px;font-size:11px;font-weight:600;
}
.card-body{padding:0;overflow-y:auto;flex:1;min-height:0}
.card-body::-webkit-scrollbar{width:4px}
.card-body::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.trades-toggle{cursor:pointer;user-select:none}
.trades-toggle:hover{background:var(--card)}
/* === RIGHT COLUMN === */
.col-right{display:flex;flex-direction:column;overflow:hidden}
.log-area{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden}
.log-header{
  display:flex;align-items:center;gap:8px;padding:10px 14px;
  border-bottom:1px solid var(--border);flex-shrink:0;background:var(--card2);
}
#log-body{
  flex:1;overflow-y:auto;padding:8px 14px;
  font-family:var(--mono);font-size:11.5px;line-height:1.75;min-height:0;
}
#log-body::-webkit-scrollbar{width:4px}
#log-body::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.log-INFO{color:var(--text)}
.log-WARN{color:var(--yellow)}
.log-ERROR{color:var(--red)}
.log-ts{color:var(--muted);margin-right:8px;font-size:10.5px}
.manual-buy{
  border-top:1px solid #1e2a4a;background:var(--card);
  flex-shrink:0;padding:11px 14px;
}
.manual-buy-head{display:flex;align-items:center;gap:6px;margin-bottom:8px}
.manual-buy-row{display:flex;align-items:center;gap:7px;flex-wrap:wrap}
.mbuy-input{
  background:var(--card2);border:1px solid var(--border);border-radius:6px;
  color:var(--text);font-size:13px;padding:6px 10px;outline:none;transition:border-color .15s;
}
.mbuy-input:focus{border-color:var(--accent)}
.mbuy-input.ticker{width:90px;text-transform:uppercase;font-weight:700;letter-spacing:.5px}
.mbuy-input.qty{width:70px}
#mbuy-status{font-size:11px;color:var(--muted);flex:1}
#watching-chips{display:flex;flex-wrap:wrap;gap:4px;margin-top:6px}
.wc-chip{
  display:inline-flex;align-items:center;gap:3px;padding:2px 8px;
  border-radius:8px;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.28);
  font-size:11px;color:var(--accent);font-weight:600;
}
/* === GEAR DRAWER === */
#cfg-backdrop{
  display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);
  z-index:300;backdrop-filter:blur(2px);
}
#cfg-backdrop.open{display:block}
#cfg-drawer{
  position:fixed;top:0;right:0;bottom:0;width:460px;
  background:var(--card);border-left:1px solid var(--border);
  z-index:301;display:flex;flex-direction:column;
  transform:translateX(100%);transition:transform .25s cubic-bezier(.4,0,.2,1);
  box-shadow:-8px 0 40px rgba(0,0,0,.7);
}
#cfg-drawer.open{transform:translateX(0)}
.drawer-head{
  display:flex;align-items:center;gap:10px;padding:15px 20px;
  border-bottom:1px solid var(--border);flex-shrink:0;background:var(--card2);
}
.drawer-body{flex:1;overflow-y:auto;padding:18px 20px}
.drawer-body::-webkit-scrollbar{width:5px}
.drawer-body::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
/* === SHARED === */
.dot{width:8px;height:8px;border-radius:50%;background:var(--muted);transition:.3s;flex-shrink:0}
.dot.running{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot.error{background:var(--red);box-shadow:0 0 6px var(--red)}
.badge{padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;letter-spacing:.5px}
.badge.paper{background:#1f3a5f;color:var(--accent)}
.badge.live{background:#3d1f1f;color:var(--red)}
.btn{
  border:none;border-radius:6px;padding:6px 14px;font-size:12px;font-weight:600;
  cursor:pointer;transition:.1s;box-shadow:0 1px 0 rgba(0,0,0,.4);
}
.btn:hover{opacity:.9}
.btn:active{transform:translateY(1px);box-shadow:none;opacity:.75}
.btn-green{background:var(--green);color:#000}
.btn-red{background:var(--red);color:#fff}
.btn-muted{background:var(--card2);color:var(--text);border:1px solid var(--border)}
.btn-sm{padding:3px 9px;font-size:11px;border-radius:5px}
.btn-icon{
  background:transparent;border:1px solid var(--border);border-radius:6px;
  color:var(--muted);padding:4px 9px;font-size:14px;cursor:pointer;transition:.15s;
}
.btn-icon:hover{border-color:var(--accent);color:var(--accent);background:rgba(88,166,255,.08)}
.spacer{flex:1}
table{width:100%;border-collapse:collapse}
th{
  font-size:10px;text-transform:uppercase;letter-spacing:.6px;
  color:var(--muted);padding:8px 12px;text-align:left;
  background:var(--card2);border-bottom:1px solid var(--border);position:sticky;top:0;
}
td{padding:8px 12px;border-bottom:1px solid var(--border);font-family:var(--mono);font-size:12px}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.015)}
.pos-green{color:var(--green)}
.pos-red{color:var(--red)}
.ticker-cell{font-weight:700;color:#fff;font-size:13px}
.action-badge{
  display:inline-block;padding:2px 7px;border-radius:4px;
  font-size:10px;font-weight:700;font-family:var(--font);
}
.action-BUY{background:#1f3a2f;color:var(--green)}
.action-SELL{background:#3d1f1f;color:var(--red)}
.action-STOP{background:#3d2a1f;color:var(--yellow)}
.action-TP{background:#1f2a3d;color:var(--accent)}
.action-TRAIL{background:#2a1f3d;color:var(--purple)}
.action-EOD{background:#2a2a2a;color:var(--muted)}
.action-PYRAMID{background:#2a1f3d;color:var(--purple)}
.empty{color:var(--muted);text-align:center;padding:24px;font-size:12px}
/* Config form */
.cfg-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-bottom:14px}
.cfg-field label{display:block;font-size:10px;color:var(--muted);margin-bottom:4px;
                  text-transform:uppercase;letter-spacing:.5px}
.cfg-field input,.cfg-field select{
  width:100%;background:var(--card2);border:1px solid var(--border);
  border-radius:6px;color:var(--text);padding:7px 9px;font-size:12px;font-family:var(--mono);
}
.cfg-field input:focus,.cfg-field select:focus{outline:none;border-color:var(--accent)}
.cfg-sep{
  font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.7px;
  margin:14px 0 8px;padding-top:12px;border-top:1px solid var(--border);
}
.cfg-sep.accent{color:var(--accent)}
.cfg-sep:first-child{border-top:none;margin-top:0;padding-top:0}
.mode-toggle{display:flex;gap:0;border:1px solid var(--border);border-radius:8px;
             overflow:hidden;width:fit-content;margin-bottom:14px}
.mode-btn{padding:7px 18px;cursor:pointer;font-size:12px;font-weight:600;transition:.15s;border:none}
.mode-btn.active-paper{background:var(--accent);color:#000}
.mode-btn.active-live{background:var(--red);color:#fff}
.mode-btn:not(.active-paper):not(.active-live){background:var(--card2);color:var(--muted)}
/* Tooltip */
#tt{
  position:fixed;background:#1a1f2e;border:1px solid #3a4468;
  border-radius:8px;padding:7px 11px;font-size:12px;color:#e0e6ff;
  pointer-events:none;z-index:9999;display:none;white-space:nowrap;
  box-shadow:0 6px 20px rgba(0,0,0,.55);line-height:1.7;
}
.src-dot{display:inline-block;width:7px;height:7px;border-radius:50%;flex-shrink:0;margin-right:2px}
/* Trending chips */
.tr-chip{
  display:inline-flex;align-items:center;
  background:var(--card2);border:1px solid var(--border);
  border-radius:12px;font-size:11px;font-family:var(--mono);font-weight:600;overflow:hidden;
}
.tr-chip .tr-tv{
  color:var(--accent);padding:3px 3px 3px 8px;cursor:pointer;text-decoration:none;
  display:inline-flex;align-items:center;gap:3px;
}
.tr-chip .tr-tv:hover{color:var(--green)}
.tr-chip .tr-test,.tr-chip .tr-add{padding:3px 5px;cursor:pointer;color:var(--muted)}
.tr-chip .tr-add:hover{background:#1f3a5f;color:var(--green)}
.tr-chip .tr-test:hover{background:#1a2a3a;color:var(--accent)}
.tr-chip .tr-sep{width:1px;background:var(--border);align-self:stretch}
.tr-chip .tr-rm{padding:3px 6px;cursor:pointer;color:var(--muted)}
.tr-chip .tr-rm:hover{background:#3d1f1f;color:var(--red)}
/* Compat hidden */
#wl-banner-chips,#wl-body,#wl-arrow,.cfg-section-label{display:none!important}
/* Watchlist banner chips (compat: src dots in header) */
.wl-bc{
  display:inline-flex;align-items:center;gap:2px;padding:1px 7px;border-radius:8px;
  background:var(--card2);border:1px solid var(--border);
  color:#fff;text-decoration:none;font-size:10px;font-weight:700;
  font-family:var(--mono);white-space:nowrap;flex-shrink:0;transition:.12s;
}
.wl-bc:hover{border-color:var(--accent);color:var(--accent)}
#error-bar{
  display:none;background:#3d1f1f;border-bottom:2px solid var(--red);
  padding:8px 20px;color:var(--red);font-size:12px;font-weight:600;flex-shrink:0;
}
@media(max-width:1200px){
  .canvas{grid-template-columns:230px 1fr 340px}
  #cfg-drawer{width:400px}
}
/* ── On Deck card ── */
.ondeck-body{padding:6px 8px;display:flex;flex-direction:column;gap:4px;min-height:36px}
.ondeck-empty{color:var(--muted);font-size:12px;padding:4px 0}
.ondeck-row{
  display:grid;grid-template-columns:64px 1fr auto;align-items:center;
  gap:8px;padding:5px 6px;border-radius:6px;
  background:var(--card2);border:1px solid var(--border);
  transition:border-color .15s;
}
.ondeck-row:hover{border-color:var(--yellow)}
.ondeck-ticker{
  font-weight:700;font-size:13px;color:var(--yellow);
  text-decoration:none;letter-spacing:.3px;white-space:nowrap;
  font-family:var(--mono);
}
.ondeck-ticker:hover{color:#fff}
.ondeck-bars{display:flex;flex-direction:column;gap:3px}
.ondeck-bar-wrap{display:flex;align-items:center;gap:5px;font-size:10px}
.ondeck-bar-lbl{color:var(--muted);width:26px;flex-shrink:0;font-family:var(--mono)}
.ondeck-bar-outer{flex:1;position:relative}
.ondeck-bar-track{
  width:100%;height:5px;background:#1c2128;border-radius:3px;overflow:hidden;
  border:1px solid var(--border);
}
.ondeck-bar-fill{
  height:100%;border-radius:3px;
  transition:width .4s ease;
}
.ondeck-bar-fill.fast{background:linear-gradient(90deg,var(--accent),var(--yellow),var(--red))}
.ondeck-bar-fill.slow{background:linear-gradient(90deg,var(--purple),var(--yellow),var(--red))}
.ondeck-threshold-mark{
  position:absolute;top:-3px;bottom:-3px;width:2px;
  background:rgba(255,255,255,0.55);border-radius:1px;
  pointer-events:none;
}
.ondeck-bar-val{color:var(--muted);width:28px;text-align:right;font-family:var(--mono);flex-shrink:0}
/* ── Signal readiness chips ── */
.sig-row{display:flex;align-items:center;gap:3px;margin-top:5px;flex-wrap:wrap}
.sig-chip{
  font-size:9px;padding:1px 5px;border-radius:3px;
  font-family:var(--mono);font-weight:600;white-space:nowrap;cursor:default;
  transition:background .3s,color .3s,border-color .3s;
}
.sig-chip.pass{background:rgba(63,185,80,.14);color:var(--green);border:1px solid rgba(63,185,80,.3)}
.sig-chip.fail{background:rgba(255,255,255,.04);color:var(--muted);border:1px solid rgba(255,255,255,.1)}
.sig-chip.warn{background:rgba(210,153,34,.14);color:var(--yellow);border:1px solid rgba(210,153,34,.3)}
.sig-score{
  font-size:10px;font-family:var(--mono);font-weight:700;
  margin-left:auto;padding-left:4px;
}
.float-badge{display:inline-block;font-size:10px;font-family:var(--mono);padding:1px 5px;border-radius:3px;margin-left:4px;vertical-align:middle}
.float-badge.micro{background:rgba(0,210,255,0.15);color:#00d2ff;border:1px solid rgba(0,210,255,0.35)}
.float-badge.large{background:rgba(120,120,140,0.12);color:var(--muted);border:1px solid rgba(120,120,140,0.2)}
.ondeck-pulse{
  width:6px;height:6px;border-radius:50%;background:var(--yellow);
  animation:pulse-y 1s infinite;flex-shrink:0;
}
@keyframes pulse-y{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.6)}}
/* ── On Deck event feed ── */
.ondeck-feed{
  margin-top:6px;border-top:1px solid var(--border);padding-top:5px;
  display:flex;flex-direction:column;gap:2px;
}
.ondeck-feed-title{
  font-size:9px;text-transform:uppercase;letter-spacing:.7px;
  color:var(--muted);font-family:var(--mono);margin-bottom:2px;
}
.ode-row{
  display:flex;align-items:flex-start;gap:6px;
  font-size:10px;line-height:1.35;padding:2px 0;
  border-bottom:1px solid rgba(255,255,255,.04);
}
.ode-row:last-child{border-bottom:none}
.ode-ts{color:var(--muted);font-family:var(--mono);flex-shrink:0;font-size:9px;margin-top:1px}
.ode-icon{flex-shrink:0;width:14px;text-align:center;margin-top:1px}
.ode-body{flex:1;min-width:0}
.ode-ticker{font-weight:700;font-family:var(--mono);margin-right:4px}
.ode-reason{color:var(--muted);font-size:9px}
.ode-conds{display:flex;gap:2px;flex-wrap:wrap;margin-top:2px}
.ode-cond{font-size:8px;font-family:var(--mono);padding:0 3px;border-radius:2px}
.ode-cond.y{background:rgba(63,185,80,.15);color:var(--green);border:1px solid rgba(63,185,80,.25)}
.ode-cond.n{background:rgba(255,255,255,.04);color:var(--muted);border:1px solid rgba(255,255,255,.08)}
/* direction colour codes */
.ode-enter .ode-ticker{color:var(--yellow)}
.ode-exit_buy .ode-ticker{color:var(--green)}
.ode-exit_fail .ode-ticker{color:var(--muted)}
.ode-exit_manual .ode-ticker{color:var(--muted)}
.ode-exit_position .ode-ticker{color:var(--accent)}
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
</head>
<body>
<!-- COMPAT HIDDEN ELEMENTS -->
<div id="wl-banner-chips"></div>
<div id="wl-body"></div>
<span id="wl-arrow"></span>
<!-- ERROR BAR -->
<div id="error-bar"></div>
<!-- HEADER -->
<header>
  <div class="hdr-logo">&#9889; <span class="ac">Alpaca</span> Bot <span class="ver">v{{BOT_VERSION}}</span></div>
  <div class="hdr-status">
    <div class="dot" id="status-dot"></div>
    <span id="status-text" style="color:var(--muted);font-size:12px">Stopped</span>
    <span class="badge paper" id="mode-badge">PAPER</span>
  </div>
  <div class="hdr-stats">
    <div class="hdr-stat">
      <div class="hdr-stat-lbl">Equity</div>
      <div class="hdr-stat-val" id="s-equity">&#8212;</div>
    </div>
    <div class="hdr-stat">
      <div class="hdr-stat-lbl">Cash</div>
      <div class="hdr-stat-val" id="s-cash">&#8212;</div>
    </div>
    <div class="hdr-stat">
      <div class="hdr-stat-lbl">Day P&amp;L</div>
      <div class="hdr-stat-val" id="s-pnl">&#8212;</div>
      <div class="hdr-stat-sub" id="s-pnl-pct"></div>
    </div>
    <div class="hdr-stat">
      <div class="hdr-stat-lbl">Buys</div>
      <div class="hdr-stat-val" id="s-trades">0</div>
      <div class="hdr-stat-sub" id="s-trades-sub">of 10 max</div>
    </div>
    <div class="hdr-stat">
      <div class="hdr-stat-lbl">Positions</div>
      <div class="hdr-stat-val" id="s-positions">0</div>
      <div class="hdr-stat-sub">of 10 max</div>
    </div>
  </div>
  <div class="hdr-controls">
    <button class="btn-icon" id="btn-test" onclick="testConn()" title="Test connection">&#128268;</button>
    <button class="btn-icon" onclick="toggleConfig()" title="Settings">&#9881;</button>
    <button class="btn btn-green" id="btn-toggle" onclick="toggleBot()">&#9654; Start</button>
  </div>
</header>
<!-- THREE-COLUMN CANVAS -->
<div class="canvas">
  <!-- LEFT: Stock Selection Tabs -->
  <div class="col-left">
    <div class="tab-strip">
      <button class="tab-btn active" id="tab-btn-wl" onclick="switchTab(\'wl\')">&#128203; Watchlist</button>
      <button class="tab-btn" id="tab-btn-tr" onclick="switchTab(\'tr\')">&#128225; Trending</button>
      <button class="tab-btn" id="tab-btn-an" onclick="switchTab(\'an\')">&#128200; Analytics</button>
    </div>
    <!-- WATCHLIST TAB -->
    <div class="tab-pane active" id="tab-pane-wl">
      <div class="wl-toolbar">
        <input id="wl-add-input" type="text" placeholder="TICKER"
               oninput="this.value=this.value.toUpperCase()"
               onkeydown="if(event.key===\'Enter\')addTicker()">
        <button class="btn btn-green btn-sm" onclick="addTicker()">+</button>
        <input id="wl-search" type="text" placeholder="Filter&hellip;" oninput="filterChips()" style="margin-left:auto">
      </div>
      <div class="wl-chips" id="wl-chips">
        <span style="color:var(--muted);font-size:12px">No tickers &mdash; add some above.</span>
      </div>
      <textarea id="wl-paste-area" placeholder="Bulk import &mdash; paste tickers (space/comma/newline)&hellip;"></textarea>
      <div class="wl-footer">
        <button class="btn btn-muted btn-sm" onclick="bulkImport()">&#8984; Import</button>
        <button class="btn btn-muted btn-sm" onclick="openAllInTV()">&#128200; TV</button>
        <button class="btn btn-green btn-sm" id="wl-save-btn" onclick="saveWatchlist()">&#128190; Save</button>
        <span id="wl-save-msg" style="font-size:11px;color:var(--green)"></span>
        <span class="spacer"></span>
        <span id="wl-count" style="font-size:11px;color:var(--muted);font-family:var(--mono)">0</span>
        <span id="wl-footer-count" style="font-size:10px;color:var(--muted)"></span>
        <button class="btn btn-sm" onclick="clearAllTickers()"
          style="color:var(--red);background:transparent;border:1px solid #3d1f1f;font-size:10px">&#10005; All</button>
      </div>
      <!-- Locked tickers -->
      <div id="locked-section" style="display:none">
        <div class="locked-bar">
          <div class="locked-lbl">&#128274; Auto-buy locked &mdash; click to unlock</div>
          <div id="locked-chips"></div>
        </div>
      </div>
    </div>
    <!-- TRENDING TAB -->
    <div class="tab-pane" id="tab-pane-tr">
      <div class="trend-tab-header">
        <span style="font-size:11px;color:var(--muted)" id="trend-updated">&#8212;</span>
        <span class="spacer"></span>
        <button class="btn btn-muted btn-sm" onclick="refreshTrending()" id="trend-btn">&#8635; Refresh</button>
      </div>
      <div id="trend-chips">
        <span style="color:var(--muted);font-size:12px">Fetching&hellip;</span>
      </div>
    </div>

    <!-- ANALYTICS TAB -->
    <div class="tab-pane" id="tab-pane-an">
      <div class="an-toolbar">
        <span style="font-size:11px;color:var(--muted)">Last</span>
        <select id="an-days" onchange="loadAnalytics()" style="background:var(--surface);color:var(--text);border:1px solid var(--border);border-radius:4px;padding:2px 6px;font-size:11px;margin-left:4px">
          <option value="30">30 days</option>
          <option value="60">60 days</option>
          <option value="90" selected>90 days</option>
          <option value="180">180 days</option>
        </select>
        <span class="spacer"></span>
        <button class="btn btn-muted btn-sm" onclick="loadAnalytics()">&#8635; Refresh</button>
      </div>
      <!-- Stat cards -->
      <div class="an-cards" id="an-cards">
        <div class="an-empty">No trade history yet &mdash; run the bot to build data.</div>
      </div>
      <!-- Equity curve canvas -->
      <div id="an-chart-wrap" style="display:none">
        <canvas id="an-equity-chart" height="120"></canvas>
      </div>
      <!-- Per-ticker table -->
      <div id="an-ticker-wrap" style="display:none">
        <div style="font-size:11px;color:var(--muted);margin:8px 0 4px">By Ticker</div>
        <table class="an-table" id="an-ticker-table">
          <thead><tr>
            <th>Ticker</th><th>Trades</th><th>Win%</th><th>Avg</th><th>Total</th>
          </tr></thead>
          <tbody id="an-ticker-body"></tbody>
        </table>
      </div>
      <!-- Recent trades list -->
      <div id="an-trades-wrap" style="display:none">
        <div style="font-size:11px;color:var(--muted);margin:8px 0 4px">Recent Exits</div>
        <table class="an-table" id="an-trades-table">
          <thead><tr>
            <th>Date</th><th>Ticker</th><th>Action</th>
            <th>P&amp;L</th><th>Streak</th><th>Float</th>
          </tr></thead>
          <tbody id="an-trades-body"></tbody>
        </table>
      </div>
    </div>

  </div>
  <!-- CENTER: Trading Activity -->
  <div class="col-center">
    <div class="card positions">
      <div class="card-header">
        <span class="card-title">Open Positions</span>
        <span class="count-badge" id="pos-badge">0</span>
      </div>
      <div class="card-body">
        <table>
          <thead>
            <tr>
              <th>Ticker</th><th>Entry</th><th>Live</th>
              <th>P&amp;L $</th><th>P&amp;L %</th><th>HW</th><th></th><th></th>
            </tr>
          </thead>
          <tbody id="pos-tbody">
            <tr><td colspan="8" class="empty">No open positions</td></tr>
          </tbody>
        </table>
      </div>
    </div>
    <div class="card on-deck">
      <div class="card-header">
        <div class="ondeck-pulse" id="ondeck-pulse" style="display:none"></div>
        <span class="card-title">On Deck</span>
        <span class="count-badge" id="ondeck-badge">0</span>
        <span class="spacer"></span>
        <span style="font-size:11px;color:var(--muted)">fast-scan watching</span>
      </div>
      <div id="ondeck-body" class="card-body ondeck-body">
        <div class="ondeck-empty">No tickers approaching signal</div>
      </div>
      <div id="ondeck-feed" class="ondeck-feed" style="display:none;padding:0 8px 6px">
        <div class="ondeck-feed-title">Recent Events</div>
        <div id="ondeck-feed-rows"></div>
      </div>
    </div>
    <div class="card trades">
      <div class="card-header trades-toggle" onclick="toggleTrades()">
        <span class="card-title">Today\'s Trades</span>
        <span class="count-badge" id="trade-badge">0</span>
        <span class="spacer"></span>
        <span id="trades-arrow" style="color:var(--muted);font-size:11px">&#9660;</span>
      </div>
      <div id="trades-collapsed-body" class="card-body">
        <table>
          <thead>
            <tr><th>Time</th><th>Ticker</th><th>Action</th><th>Qty</th><th>Entry $</th><th>Exit $</th><th>P&amp;L</th></tr>
          </thead>
          <tbody id="trades-tbody">
            <tr><td colspan="7" class="empty">No trades today</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <!-- RIGHT: Log + Manual Buy -->
  <div class="col-right">
    <div class="log-area">
      <div class="log-header">
        <span style="font-weight:700;color:#fff;font-size:13px">Live Log</span>
        <span class="count-badge" id="last-scan-ts" style="font-family:var(--mono)">&#8212;</span>
        <span class="spacer"></span>
        <button class="btn btn-muted btn-sm" onclick="clearLog()">Clear</button>
      </div>
      <div id="log-body"></div>
    </div>
    <div class="manual-buy">
      <div class="manual-buy-head">
        <span style="font-size:13px">&#128722;</span>
        <span style="font-weight:700;color:#fff;font-size:12px">Manual Buy</span>
        <span style="font-size:10px;color:var(--muted)">&middot; market order</span>
        <span class="spacer"></span>
      </div>
      <div class="manual-buy-row">
        <input class="mbuy-input ticker" id="mbuy-ticker" type="text" placeholder="TICKER"
               oninput="this.value=this.value.toUpperCase()"
               onkeydown="if(event.key===\'Enter\')document.getElementById(\'mbuy-qty\').focus()">
        <input class="mbuy-input qty" id="mbuy-qty" type="number" placeholder="Qty" min="1" step="1"
               onkeydown="if(event.key===\'Enter\')manualBuy()">
        <button class="btn btn-green" onclick="manualBuy()">&#9654; Buy</button>
        <span id="mbuy-status"></span>
      </div>
      <div id="watching-chips" style="display:none"></div>
    </div>
  </div>
</div>
<!-- GEAR DRAWER BACKDROP -->
<div id="cfg-backdrop" onclick="toggleConfig()"></div>
<!-- GEAR DRAWER -->
<div id="cfg-drawer">
  <div class="drawer-head">
    <span style="font-size:16px">&#9881;</span>
    <span style="font-weight:700;color:#fff;font-size:14px">Configuration</span>
    <span class="spacer"></span>
    <button class="btn btn-muted btn-sm" onclick="toggleConfig()">&#10005; Close</button>
  </div>
  <div class="drawer-body">
    <div class="cfg-sep accent">Mode</div>
    <div class="mode-toggle">
      <button class="mode-btn active-paper" id="btn-paper" onclick="setMode(\'paper\')">Paper Trading</button>
      <button class="mode-btn" id="btn-live" onclick="setMode(\'live\')">Live Trading</button>
    </div>
    <div class="cfg-sep">API Credentials</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>API Key</label>
        <input type="password" id="cfg-api-key" placeholder="PK&hellip;"></div>
      <div class="cfg-field"><label>Secret Key</label>
        <input type="password" id="cfg-secret-key" placeholder="secret&hellip;"></div>
    </div>
    <div class="cfg-sep">Risk &amp; Sizing</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>Stop Loss %</label>
        <input type="number" id="cfg-stop" step="0.1" min="0.5" max="20"></div>
      <div class="cfg-field"><label>Take Profit %</label>
        <input type="number" id="cfg-tp" step="0.5" min="1" max="50"></div>
      <div class="cfg-field"><label>Position Size %</label>
        <input type="number" id="cfg-pos-size" step="1" min="1" max="50"></div>
      <div class="cfg-field"><label>Trail Activate %</label>
        <input type="number" id="cfg-trail-act" step="0.5" min="1" max="20"></div>
      <div class="cfg-field"><label>Trail Stop %</label>
        <input type="number" id="cfg-trail-pct" step="0.5" min="1" max="15"></div>
      <div class="cfg-field"><label>Scan Interval (sec)</label>
        <input type="number" id="cfg-interval" step="10" min="30"></div>
      <div class="cfg-field"><label>Bar Timeframe</label>
        <select id="cfg-bar">
          <option value="1Min">1 Min</option><option value="5Min">5 Min</option>
          <option value="15Min">15 Min</option><option value="1Hour">1 Hour</option>
        </select></div>
      <div class="cfg-field"><label>Pyramid</label>
        <select id="cfg-pyramid">
          <option value="true">Enabled</option><option value="false">Disabled</option>
        </select></div>
      <div class="cfg-field"><label>Pyramid Gain %</label>
        <input type="number" id="cfg-pyr-gain" step="0.5" min="1" max="20"></div>
    </div>
    <div class="cfg-sep">Partial Exit</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>Partial Exit</label>
        <select id="cfg-partial-on">
          <option value="true">Enabled</option><option value="false">Disabled</option>
        </select></div>
      <div class="cfg-field"><label>Take Partial At %</label>
        <input type="number" id="cfg-partial-pct" step="0.5" min="1" max="30" placeholder="6"></div>
      <div class="cfg-field"><label>Qty to Sell %</label>
        <input type="number" id="cfg-partial-qty" step="5" min="10" max="90" placeholder="50"></div>
    </div>
    <div class="cfg-sep">Trending Sources</div>
    <div class="cfg-grid" style="grid-template-columns:1fr 1fr">
      <div class="cfg-field"><label>Max Price ($)</label>
        <input type="number" id="cfg-trend-price" step="0.5" min="0.5" max="500" placeholder="5.00"></div>
    </div>
    <div class="cfg-sep">Float &amp; Volume Filters</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>Float Filter</label>
        <select id="cfg-float-on">
          <option value="true">Enabled</option><option value="false">Disabled</option>
        </select></div>
      <div class="cfg-field"><label>Max Float (M shares)</label>
        <input type="number" id="cfg-float-max" step="5" min="1" max="500" placeholder="50"></div>
      <div class="cfg-field"><label>Micro Float ⚡ (M)</label>
        <input type="number" id="cfg-micro-float" step="1" min="1" max="100" placeholder="10" title="On Deck: floats at or below this get ⚡ badge and top priority"></div>
      <div class="cfg-field"><label>Daily RVOL Filter</label>
        <select id="cfg-rvol-on">
          <option value="true">Enabled</option><option value="false">Disabled</option>
        </select></div>
      <div class="cfg-field"><label>Min RVOL (x avg)</label>
        <input type="number" id="cfg-rvol-min" step="0.5" min="0.5" max="10" placeholder="2.0"></div>
    </div>
    <div class="cfg-sep">Auto-Schedule (ET)</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>Auto-Schedule</label>
        <select id="cfg-auto-sched">
          <option value="true">Enabled</option><option value="false">Disabled</option>
        </select></div>
      <div class="cfg-field"><label>Start Time HH / MM</label>
        <div style="display:flex;gap:4px">
          <input type="number" id="cfg-open-h" min="6" max="12" step="1" style="width:54px" placeholder="9">
          <input type="number" id="cfg-open-m" min="0" max="59" step="1" style="width:54px" placeholder="15">
        </div></div>
    </div>
    <div id="sched-status" style="margin-bottom:12px;font-size:11px;color:var(--muted)"></div>
    <div class="cfg-sep accent">&#9889; Strategy</div>
    <div class="cfg-grid" style="grid-template-columns:1fr">
      <div class="cfg-field"><label>Strategy</label>
        <select id="cfg-strategy" onchange="onStrategyChange()">
          <option value="exhaustion">%R Exhaustion (default)</option>
          <option value="ema_crossover">EMA Crossover</option>
          <option value="macd">MACD Only</option>
        </select></div>
    </div>
    <div id="strat-exhaustion-section">
      <div class="cfg-sep">%R Exhaustion Parameters</div>
      <div class="cfg-grid">
        <div class="cfg-field"><label>RTE Side</label>
          <select id="cfg-rte-side">
            <option value="red">Red (Overbought)</option>
            <option value="blue">Blue (Oversold)</option>
          </select></div>
        <div class="cfg-field"><label>RTE Threshold (0&ndash;50)</label>
          <input type="number" id="cfg-rte-threshold" step="1" min="5" max="50" placeholder="20"></div>
        <div class="cfg-field"><label>Avg Formula MA</label>
          <input type="number" id="cfg-rte-avg-ma" step="1" min="1" max="10" placeholder="3"></div>
        <div class="cfg-field"><label>Min Reversals (Boxes)</label>
          <input type="number" id="cfg-rte-boxes" step="1" min="1" max="10" placeholder="3"></div>
        <div class="cfg-field"><label>RSI-2 Oversold (&lt;)</label>
          <input type="number" id="cfg-cm-rsi-thresh" step="1" min="1" max="30" placeholder="10"></div>
      </div>
      <div class="cfg-sep">Component Toggles</div>
      <div class="cfg-grid">
        <div class="cfg-field"><label>%R Exhaustion</label>
          <select id="cfg-use-rte">
            <option value="true">Enabled</option><option value="false">Disabled</option>
          </select></div>
        <div class="cfg-field"><label>RSI-2 Signal</label>
          <select id="cfg-use-cm-rsi">
            <option value="true">Enabled</option><option value="false">Disabled</option>
          </select></div>
        <div class="cfg-field"><label>Volume Trending Up</label>
          <select id="cfg-use-vol-trend">
            <option value="true">Enabled</option><option value="false">Disabled</option>
          </select></div>
        <div class="cfg-field"><label>MACD</label>
          <select id="cfg-use-macd">
            <option value="true">Enabled</option><option value="false">Disabled</option>
          </select></div>
      </div>
    </div>
    <div id="strat-macd-section">
      <div class="cfg-sep">MACD Parameters</div>
      <div class="cfg-grid">
        <div class="cfg-field"><label>Fast Period</label>
          <input type="number" id="cfg-macd-fast" step="1" min="3" max="50" placeholder="12"></div>
        <div class="cfg-field"><label>Slow Period</label>
          <input type="number" id="cfg-macd-slow" step="1" min="5" max="100" placeholder="26"></div>
        <div class="cfg-field"><label>Signal Period</label>
          <input type="number" id="cfg-macd-signal" step="1" min="2" max="30" placeholder="9"></div>
      </div>
    </div>
    <div class="cfg-sep">&#127749; Pre-Market Trading</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>Enable Pre-Market</label>
        <select id="cfg-pm-enabled">
          <option value="false">Off (default)</option><option value="true">On</option>
        </select></div>
      <div class="cfg-field"><label>Session Start ET HH/MM</label>
        <div style="display:flex;gap:6px">
          <input type="number" id="cfg-pm-h" min="1" max="9" style="width:56px" placeholder="4">
          <input type="number" id="cfg-pm-m" min="0" max="59" style="width:56px" placeholder="00">
        </div></div>
      <div class="cfg-field"><label>Limit Price Offset %</label>
        <input type="number" id="cfg-pm-offset" step="0.01" min="0" max="1" placeholder="0.20"></div>
      <div class="cfg-field"><label>PM Volume Mult</label>
        <input type="number" id="cfg-pm-vol-mult" step="0.05" min="0" max="5" placeholder="0.30"></div>
    </div>
    <div class="cfg-sep">&#128737; Capital Protection</div>
    <div class="cfg-grid">
      <div class="cfg-field"><label>Daily Max Loss %</label>
        <input type="number" id="cfg-max-daily-loss" step="0.5" min="1" max="20" placeholder="5"></div>
      <div class="cfg-field"><label>Volume Surge Mult</label>
        <input type="number" id="cfg-vol-surge" step="0.1" min="0.5" max="10" placeholder="1.5"></div>
      <div class="cfg-field"><label>Bar Count</label>
        <input type="number" id="cfg-bar-count" step="50" min="100" max="2000" placeholder="800"></div>
    </div>
    <div class="cfg-sep">Debug</div>
    <div class="cfg-grid" style="grid-template-columns:1fr">
      <div class="cfg-field"><label>Signal Debug Log</label>
        <select id="cfg-debug-signals">
          <option value="false">Off (default)</option><option value="true">On</option>
        </select></div>
    </div>
    <div class="cfg-sep">TradingView Chart</div>
    <div class="cfg-grid" style="grid-template-columns:1fr">
      <div class="cfg-field"><label>Chart Base URL</label>
        <input type="text" id="cfg-tv-url" placeholder="https://www.tradingview.com/chart/x04Gfcu8/" style="font-size:11px"></div>
    </div>
    <div style="margin-top:18px;padding-top:14px;border-top:1px solid var(--border);display:flex;align-items:center;gap:12px">
      <button class="btn btn-green" onclick="saveConfig()">&#128190; Save &amp; Apply</button>
      <span id="cfg-save-msg" style="font-size:12px;color:var(--green)"></span>
    </div>
  </div>
</div>

<!-- inline health check: runs before the main script -->
<script>
  window._dashboardLoaded = false;
  window.onerror = function(msg, src, line, col, err) {
    var bar = document.getElementById('error-bar');
    if (bar) {
      bar.style.display = 'block';
      bar.textContent = '⚠ JS Error at line ' + line + ': ' + msg;
    }
    console.error('JS ERROR:', msg, 'line:', line, err);
    return false;
  };
</script>
<script>
const $ = id => document.getElementById(id)
let cfgOpen = false
let currentMode = 'paper'
let localLogs = []

// ── SSE ──────────────────────────────────────────
const es = new EventSource('/events')
es.onmessage = e => {
  try {
    updateUI(JSON.parse(e.data))
  } catch(err) {
    console.error('[SSE] updateUI error:', err)
    const eb = $('error-bar')
    if (eb) { eb.style.display='block'; eb.textContent='⚠ Dashboard JS error: '+err.message }
  }
}
es.onerror = () => {
  const dot = $('status-dot'); if(dot) dot.className='dot error'
  const st  = $('status-text'); if(st)  { st.textContent='Disconnected'; st.style.color='var(--red)' }
}

function updateUI(d) {
  _botRunning = !!d.running   // keep toggle button logic in sync
  // Status
  const dot  = $('status-dot')
  const stxt = $('status-text')
  const tbtn = $('btn-toggle')
  if (d.running) {
    dot.className = 'dot running'
    stxt.textContent = 'Running'
    stxt.style.color = 'var(--green)'
    if (tbtn && !tbtn._busy) {
      tbtn.textContent = '■ Stop'
      tbtn.className   = 'btn btn-red'
    }
  } else {
    dot.className = d.error ? 'dot error' : 'dot'
    stxt.textContent = d.error ? 'Error' : 'Stopped'
    stxt.style.color = d.error ? 'var(--red)' : 'var(--muted)'
    if (tbtn && !tbtn._busy) {
      tbtn.textContent = '▶ Start'
      tbtn.className   = 'btn btn-green'
    }
  }

  // Mode badge
  const paper = d.paper
  const badge = $('mode-badge')
  badge.textContent = paper ? 'PAPER' : 'LIVE'
  badge.className   = 'badge ' + (paper ? 'paper' : 'live')
  currentMode       = paper ? 'paper' : 'live'
  updateModeBtns()

  // Account stats
  const acc = d.account || {}
  const eq  = acc.equity     || 0
  const ca  = acc.cash       || 0
  const le  = acc.last_equity|| eq
  const pnl = eq - le
  const pp  = le ? pnl/le*100 : 0
  $('s-equity').textContent = '$' + fmtNum(eq)
  $('s-cash').textContent   = '$' + fmtNum(ca)
  const pnlEl = $('s-pnl')
  pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + fmtNum(Math.abs(pnl))
  pnlEl.className   = 'stat-value ' + (pnl >= 0 ? 'pos' : 'neg')
  $('s-pnl-pct').textContent = (pnl >= 0 ? '+' : '') + pp.toFixed(2) + '%'

  // Trades
  const buys = d.daily_buys || 0
  const maxb  = d.max_daily_buys || 10
  $('s-trades').textContent     = buys
  $('s-trades-sub').textContent = `of ${maxb} max`
  if (d.today_trades !== undefined) $('trade-badge').textContent = d.today_trades.length

  // Positions
  const pos = d.positions || []
  $('s-positions').textContent = pos.length
  $('pos-badge').textContent   = pos.length
  renderPositions(pos)

  // Today trades — only re-render when payload includes them (on change)
  if (d.today_trades !== undefined) renderTrades(d.today_trades)

  // Logs — compare last entry (not length) so the deque rolling at 300 still triggers re-render
  const lines = d.log_lines || []
  const lastNew = lines.length ? (lines[lines.length-1].ts + lines[lines.length-1].msg) : ''
  const lastOld = localLogs.length ? (localLogs[localLogs.length-1].ts + localLogs[localLogs.length-1].msg) : ''
  if (lastNew !== lastOld) {
    localLogs = lines
    renderLog(lines)
  }
  // Show timestamp of the most recent log entry next to the "Live Log" header
  if (lines.length) $('last-scan-ts').textContent = lines[lines.length - 1].ts

  // Error
  if (d.error) showError(d.error)
  else         showError('')

  // Trending bar
  syncTrendingFromState(d)

  // Locked tickers
  renderLockedTickers(d.manually_closed || [])

  // On Deck — fast-scan watch set
  renderOnDeck(d.watching || {}, d.config || {})
  if (d.ondeck_events) renderOnDeckEvents(d.ondeck_events)

  // Populate config on first receive or when config changes (save)
  if (d.config) { populateCfg(d.config); cfgPopulated = true }
}

let cfgPopulated = false
let watchlistEdited = false   // true when user has unsaved adds/removes/edits

// ── Positions table ──────────────────────────────
let _tvChartUrl = 'https://www.tradingview.com/chart/x04Gfcu8/'

function tvUrl(ticker) {
  const base = (_tvChartUrl || 'https://www.tradingview.com/chart/x04Gfcu8/').replace(/\/?$/, '/')
  return base + '?symbol=' + encodeURIComponent(ticker)
}

function renderPositions(positions) {
  const tb = $('pos-tbody')
  if (!positions.length) {
    tb.innerHTML = '<tr><td colspan="8" class="empty">No open positions</td></tr>'
    return
  }
  tb.innerHTML = positions.map(p => {
    const pnlCls = p.pnl_dollars >= 0 ? 'pos-green' : 'pos-red'
    const sign   = p.pnl_dollars >= 0 ? '+' : ''
    return `<tr>
      <td><span class="ticker-cell">${p.ticker}</span></td>
      <td>$${p.entry.toFixed(4)}</td>
      <td>$${p.live.toFixed(4)}</td>
      <td class="${pnlCls}">${sign}$${p.pnl_dollars.toFixed(2)}</td>
      <td class="${pnlCls}">${sign}${p.pnl_pct.toFixed(2)}%</td>
      <td>$${p.high_water.toFixed(4)}</td>
      <td><a href="${tvUrl(p.ticker)}" target="_blank" title="Open ${p.ticker} on TradingView"
          style="color:var(--accent);text-decoration:none;font-size:16px">📈</a></td>
      <td><button class="btn btn-red btn-sm" onclick="closePos('${p.ticker}')">Close</button></td>
    </tr>`
  }).join('')
}

// ── Trades table ─────────────────────────────────
function renderTrades(trades) {
  const tb = $('trades-tbody')
  if (!trades.length) {
    tb.innerHTML = '<tr><td colspan="7" class="empty">No trades today</td></tr>'
    return
  }
  const SELL_ACTIONS = new Set(['SELL','STOP','TP','TRAIL','EOD','PARTIAL'])
  const rev = [...trades].reverse()
  tb.innerHTML = rev.map(t => {
    const isSell    = SELL_ACTIONS.has(t.action)
    const price     = parseFloat(t.price       || 0)
    const entryP    = parseFloat(t.entry_price || 0)
    // Entry $ = buy price; Exit $ = sell price
    const entryCell = isSell
      ? (entryP ? '$' + entryP.toFixed(4) : '—')
      : '$' + price.toFixed(4)
    const exitCell  = isSell
      ? '$' + price.toFixed(4)
      : '<span style="color:var(--muted)">—</span>'
    const pnl    = t.pnl_dollars ? (parseFloat(t.pnl_dollars) >= 0 ? '+$' : '-$') + Math.abs(parseFloat(t.pnl_dollars)).toFixed(2) : '—'
    const pnlCls = t.pnl_dollars ? (parseFloat(t.pnl_dollars) >= 0 ? 'pos-green' : 'pos-red') : ''
    return `<tr>
      <td>${t.time || '—'}</td>
      <td><strong>${t.ticker}</strong></td>
      <td><span class="action-badge action-${t.action}">${t.action}</span></td>
      <td>${t.qty}</td>
      <td>${entryCell}</td>
      <td>${exitCell}</td>
      <td class="${pnlCls}">${pnl}</td>
    </tr>`
  }).join('')
}

// ── Log ──────────────────────────────────────────
function renderLog(lines) {
  const el  = $('log-body')
  const atBottom = el.scrollHeight - el.scrollTop <= el.clientHeight + 40
  el.innerHTML = lines.map(l =>
    `<div class="log-${l.level}"><span class="log-ts">${l.ts}</span>${escHtml(l.msg)}</div>`
  ).join('')
  if (atBottom) el.scrollTop = el.scrollHeight
}

function clearLog() { localLogs = []; $('log-body').innerHTML = '' }

// ── Manual Buy ────────────────────────────────────
async function manualBuy() {
  const ticker = $('mbuy-ticker').value.trim().toUpperCase()
  const qty    = parseInt($('mbuy-qty').value, 10)
  const status = $('mbuy-status')
  if (!ticker) { status.textContent = '⚠ Enter a ticker'; status.style.color = 'var(--yellow)'; return }
  if (!qty || qty <= 0) { status.textContent = '⚠ Enter qty > 0'; status.style.color = 'var(--yellow)'; return }
  status.textContent = `⏳ Submitting ${qty} × ${ticker}…`
  status.style.color = 'var(--muted)'
  try {
    const r = await fetch('/api/manual_buy', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ticker, qty}),
    })
    const j = await r.json()
    if (j.ok) {
      status.textContent = `✓ Bought ${qty} × ${ticker}`
      status.style.color = 'var(--green)'
      $('mbuy-ticker').value = ''
      $('mbuy-qty').value    = ''
      setTimeout(() => { status.textContent = '' }, 4000)
    } else {
      status.textContent = `✗ ${j.error || 'Order failed'}`
      status.style.color = 'var(--red)'
    }
  } catch(e) {
    status.textContent = `✗ Network error`
    status.style.color = 'var(--red)'
  }
}

// ── Signal readiness helpers ──────────────────────
function sigChip(id, pass, warn, label, tip) {
  const cls = pass ? 'pass' : warn ? 'warn' : 'fail'
  return `<span class="sig-chip ${cls}" data-cond="${id}" title="${tip}">${label}</span>`
}
function sigScore(n, total) {
  const pct = n / total
  const col  = pct >= 1 ? 'var(--green)' : pct >= 0.6 ? 'var(--yellow)' : 'var(--muted)'
  return `<span class="sig-score" data-cond="score" style="color:${col}">${n}/${total}</span>`
}
function buildSigRow(info, cfg) {
  if (!info || info.reversal === undefined) return ''   // conditions not yet populated
  const minBoxes  = info.min_boxes  ?? cfg?.rte_min_boxes ?? 2
  const minSup    = info.min_support ?? cfg?.rte_min_supporting ?? 1
  const streak    = info.streak  ?? 0
  const rmiVal    = info.rmi_val != null ? info.rmi_val.toFixed(0) : '?'
  const rmiThresh = cfg?.rmi_oversold ?? 10
  // Primary (both required for signal)
  const c1 = sigChip('rev',   info.reversal,  false, 'Rev',         `Reversal bar fired (within entry window)`)
  const c2 = sigChip('boxes', info.streak_ok, false, `Box ${streak}/${minBoxes}`, `Streak: ${streak} boxes — need ${minBoxes}`)
  // Supporting (need min_support of 3)
  const c3 = sigChip('rmi',  info.rmi_ok,  false, `RMI ${rmiVal}`, `RSI-2 must be < ${rmiThresh} (currently ${rmiVal})`)
  const c4 = sigChip('vol',  info.vol_ok,  false, 'Vol',           'Volume trending up or surging')
  const c5 = sigChip('macd', info.macd_ok, false, 'MACD',          'MACD line above signal line')
  // Score: primary=2, supporting=3 → 5 total
  const passes = [info.reversal, info.streak_ok, info.rmi_ok, info.vol_ok, info.macd_ok].filter(Boolean).length
  const score  = sigScore(passes, 5)
  return `<div class="sig-row">${c1}${c2}<span style="color:var(--border);font-size:9px">│</span>${c3}${c4}${c5}${score}</div>`
}

// ── On Deck — fast-scan watching set ─────────────
function renderOnDeck(watching, cfg) {
  const body   = $('ondeck-body')
  const badge  = $('ondeck-badge')
  const pulse  = $('ondeck-pulse')
  if (!body) return

  const microThr   = (cfg && cfg.micro_float_threshold) ? cfg.micro_float_threshold : 10
  const precheckThr = cfg && cfg.precheck_threshold != null ? cfg.precheck_threshold : -40
  const threshPct   = Math.max(0, Math.min(100, precheckThr + 100)).toFixed(1)

  const entries = Object.entries(watching || {})
  _onDeckCount  = entries.length   // keep pollOnDeck guard in sync
  badge.textContent = entries.length
  if (pulse) pulse.style.display = entries.length ? 'block' : 'none'

  if (!entries.length) {
    body.innerHTML = '<div class="ondeck-empty">No tickers approaching signal</div>'
    return
  }

  // Sort priority: micro+buyReady → micro only → buyReady → rest
  // Secondary: streak desc. Tertiary: fast %R proximity (higher = closer to overbought)
  entries.sort((a, b) => {
    const ia = a[1], ib = b[1]
    const sa = ia.streak || 0, sb = ib.streak || 0
    const fa = ia.float_m || 0, fb = ib.float_m || 0
    const microA = fa > 0 && fa <= microThr
    const microB = fb > 0 && fb <= microThr
    const buyA = sa >= 2, buyB = sb >= 2
    const scoreA = (microA && buyA) ? 4 : microA ? 3 : buyA ? 2 : 1
    const scoreB = (microB && buyB) ? 4 : microB ? 3 : buyB ? 2 : 1
    if (scoreB !== scoreA) return scoreB - scoreA
    if (sb !== sa) return sb - sa
    return (ib.rte_fast || -100) - (ia.rte_fast || -100)
  })

  body.innerHTML = entries.map(([ticker, info]) => {
    const fast    = info.rte_fast != null ? info.rte_fast : -100
    const slow    = info.rte_slow != null ? info.rte_slow : -100
    const streak  = info.streak   != null ? info.streak   : 1
    const floatM  = info.float_m  != null ? info.float_m  : 0
    const fastPct = Math.max(0, Math.min(100, fast + 100)).toFixed(1)
    const slowPct = Math.max(0, Math.min(100, slow + 100)).toFixed(1)
    const url      = tvUrl(ticker)
    const buyReady = streak >= 2
    const isMicro  = floatM > 0 && floatM <= microThr

    // Row border: green if buy-ready, cyan tint if micro only
    const rowStyle = buyReady
      ? 'border-color:var(--green)'
      : isMicro ? 'border-color:rgba(0,210,255,0.5)' : ''

    const streakStyle = buyReady ? 'color:var(--green);font-weight:700' : 'color:var(--yellow)'
    const streakLabel = buyReady ? '&#9654; ' + streak : String(streak)

    // Float badge: ⚡ cyan for micro, plain gray for larger, hidden if unknown
    const floatLabel = floatM >= 1000
      ? (floatM / 1000).toFixed(1) + 'B'
      : floatM >= 1 ? floatM.toFixed(1) + 'M' : ''
    const floatBadge = floatLabel
      ? `<span class="float-badge ${isMicro ? 'micro' : 'large'}">${isMicro ? '⚡ ' : ''}${floatLabel}</span>`
      : ''

    return `
    <div class="ondeck-row" style="${rowStyle}">
      <div style="display:flex;flex-direction:column;gap:2px;min-width:64px">
        <div style="display:flex;align-items:center;gap:0">
          <a class="ondeck-ticker" href="${url}" target="_blank">${ticker}</a>${floatBadge}
        </div>
        <span data-streak style="font-size:10px;${streakStyle};font-family:var(--mono)">${streakLabel} box${streak !== 1 ? 'es' : ''}</span>
      </div>
      <div class="ondeck-bars">
        <div class="ondeck-bar-wrap">
          <span class="ondeck-bar-lbl">Fast</span>
          <div class="ondeck-bar-outer">
            <div class="ondeck-bar-track"><div class="ondeck-bar-fill fast" style="width:${fastPct}%"></div></div>
            <div class="ondeck-threshold-mark" style="left:${threshPct}%" title="precheck threshold (${Math.round(precheckThr)})"></div>
          </div>
          <span class="ondeck-bar-val">${Math.round(fast)}</span>
        </div>
        <div class="ondeck-bar-wrap">
          <span class="ondeck-bar-lbl">Slow</span>
          <div class="ondeck-bar-outer">
            <div class="ondeck-bar-track"><div class="ondeck-bar-fill slow" style="width:${slowPct}%"></div></div>
            <div class="ondeck-threshold-mark" style="left:${threshPct}%" title="precheck threshold (${Math.round(precheckThr)})"></div>
          </div>
          <span class="ondeck-bar-val">${Math.round(slow)}</span>
        </div>
        ${buildSigRow(info, cfg)}
      </div>
      <button onclick="removeFromOnDeck('${ticker}')" title="Dismiss from fast-scan"
        style="margin-left:4px;background:none;border:none;cursor:pointer;color:var(--muted);
               font-size:14px;line-height:1;padding:2px 4px;border-radius:4px;flex-shrink:0"
        onmouseover="this.style.color='var(--red)';this.style.background='rgba(248,81,73,.12)'"
        onmouseout="this.style.color='var(--muted)';this.style.background='none'">&#x2715;</button>
    </div>`
  }).join('')
}

async function removeFromOnDeck(ticker) {
  try {
    await fetch('/api/watching/remove/' + ticker, {method: 'POST'})
  } catch(e) {}
}

// ── On Deck live-update poller (1 s) ─────────────
// Updates bar widths and values in-place without a full re-render.
// A full re-render (from SSE) handles adds/removes; this just refreshes numbers.
let _onDeckPollActive = false
let _onDeckCount = 0   // updated by renderOnDeck — skip poll when 0
async function pollOnDeck() {
  // Don't hit the server when there's nothing to update
  if (!_botRunning || _onDeckCount === 0) return
  if (_onDeckPollActive) return
  _onDeckPollActive = true
  try {
    const r = await fetch('/api/watching')
    if (!r.ok) return
    const data = await r.json()
    const watching = data.watching || {}
    const body = $('ondeck-body')
    if (!body) return
    for (const [ticker, info] of Object.entries(watching)) {
      const fast   = info.rte_fast != null ? info.rte_fast : -100
      const slow   = info.rte_slow != null ? info.rte_slow : -100
      const streak = info.streak   != null ? info.streak   : 1
      const fastPct = Math.max(0, Math.min(100, fast + 100)).toFixed(1)
      const slowPct = Math.max(0, Math.min(100, slow + 100)).toFixed(1)
      // Find this ticker's row bars by ticker label
      const rows = body.querySelectorAll('.ondeck-row')
      for (const row of rows) {
        const lbl = row.querySelector('.ondeck-ticker')
        if (!lbl || lbl.textContent.trim() !== ticker) continue
        const fills = row.querySelectorAll('.ondeck-bar-fill')
        const vals  = row.querySelectorAll('.ondeck-bar-val')
        if (fills[0]) fills[0].style.width = fastPct + '%'
        if (fills[1]) fills[1].style.width = slowPct + '%'
        if (vals[0])  vals[0].textContent  = Math.round(fast)
        if (vals[1])  vals[1].textContent  = Math.round(slow)
        // Update streak label
        const streakEl = row.querySelector('[data-streak]')
        if (streakEl) streakEl.textContent = streak + ' box' + (streak !== 1 ? 'es' : '')
        // Update signal condition chips in-place
        const condMap = {
          rev:   [info.reversal,  null],
          boxes: [info.streak_ok, null],
          rmi:   [info.rmi_ok,    null],
          vol:   [info.vol_ok,    null],
          macd:  [info.macd_ok,   null],
        }
        for (const [id, [pass]] of Object.entries(condMap)) {
          const chip = row.querySelector(`[data-cond="${id}"]`)
          if (!chip) continue
          chip.className = 'sig-chip ' + (pass ? 'pass' : 'fail')
          // Update RMI chip label with live value
          if (id === 'rmi' && info.rmi_val != null)
            chip.textContent = 'RMI ' + Math.round(info.rmi_val)
          // Update boxes chip label with live streak
          if (id === 'boxes')
            chip.textContent = 'Box ' + streak + '/' + (info.min_boxes ?? 2)
        }
        // Update score
        const scoreEl = row.querySelector('[data-cond="score"]')
        if (scoreEl) {
          const passes = [info.reversal, info.streak_ok, info.rmi_ok, info.vol_ok, info.macd_ok].filter(Boolean).length
          const col = passes >= 5 ? 'var(--green)' : passes >= 3 ? 'var(--yellow)' : 'var(--muted)'
          scoreEl.textContent = passes + '/5'
          scoreEl.style.color = col
        }
        break
      }
    }
  } catch(e) {}
  finally { _onDeckPollActive = false }
}
setInterval(pollOnDeck, 1000)

// keep old name as no-op for safety
function renderWatchingChips() {}

// ── On Deck event feed ────────────────────────────
const _ODE_ICONS = {
  enter:         '⟶',
  exit_buy:      '★',
  exit_fail:     '✕',
  exit_manual:   '⊘',
  exit_position: '⚡',
}
const _ODE_COND_LABELS = {
  reversal: 'REV', streak_ok: 'BOX', rmi_ok: 'RMI', vol_ok: 'VOL', macd_ok: 'MACD',
  rte_fast: null, rte_slow: null, streak: null, float_m: null, micro: null,
}
let _lastOdeEvents = []
function renderOnDeckEvents(events) {
  if (!events || !events.length) return
  // Compare by ts+ticker of last event to skip unnecessary re-render
  const lastNew = events.length ? (events[events.length-1].ts + events[events.length-1].ticker) : ''
  const lastOld = _lastOdeEvents.length ? (_lastOdeEvents[_lastOdeEvents.length-1].ts + _lastOdeEvents[_lastOdeEvents.length-1].ticker) : ''
  if (lastNew === lastOld) return
  _lastOdeEvents = events

  const feed = $('ondeck-feed')
  const rows = $('ondeck-feed-rows')
  if (!feed || !rows) return

  // Build rows newest-first
  const html = [...events].reverse().map(ev => {
    const dir    = ev.direction || 'enter'
    const icon   = _ODE_ICONS[dir] || '·'
    const conds  = ev.conditions || {}
    // Build condition chips (only boolean-typed ones)
    const chips  = Object.entries(_ODE_COND_LABELS)
      .filter(([k, lbl]) => lbl && typeof conds[k] === 'boolean')
      .map(([k, lbl]) => `<span class="ode-cond ${conds[k] ? 'y' : 'n'}">${lbl}</span>`)
      .join('')
    // For "enter" events show rte values instead of pass/fail chips
    const enterInfo = dir === 'enter'
      ? `<span class="ode-cond y">f=${Math.round(conds.rte_fast ?? -100)}</span>` +
        `<span class="ode-cond y">s=${Math.round(conds.rte_slow ?? -100)}</span>` +
        (conds.micro ? '<span class="ode-cond y">⚡MICRO</span>' : '')
      : chips
    return `<div class="ode-row ode-${dir}">
      <span class="ode-ts">${ev.ts}</span>
      <span class="ode-icon">${icon}</span>
      <div class="ode-body">
        <span class="ode-ticker">${ev.ticker}</span><span class="ode-reason">${ev.reason || ''}</span>
        ${enterInfo ? `<div class="ode-conds">${enterInfo}</div>` : ''}
      </div>
    </div>`
  }).join('')

  rows.innerHTML = html || '<div style="color:var(--muted);font-size:10px">No events yet</div>'
  feed.style.display = events.length ? '' : 'none'
}

// ── Bot control ──────────────────────────────────
let _botRunning = false   // mirrors d.running from SSE, kept in sync by updateUI

async function toggleBot() {
  const btn  = $('btn-toggle')
  const dot  = $('status-dot')
  const stxt = $('status-text')
  btn._busy  = true
  if (_botRunning) {
    btn.textContent = '⏳ Stopping…'
    btn.className   = 'btn btn-muted'
    await fetch('/api/stop', {method:'POST'})
  } else {
    btn.textContent = '⏳ Starting…'
    btn.className   = 'btn btn-muted'
    dot.className   = 'dot'
    stxt.textContent = 'Connecting…'
    stxt.style.color = 'var(--yellow)'
    try {
      const r = await fetch('/api/start', {method:'POST'})
      const j = await r.json()
      if (!j.ok) showError('Start failed: ' + (j.msg || 'unknown error'))
    } catch(e) {
      showError('Could not reach server: ' + e.message)
    }
  }
  // Let SSE state clear the busy flag and update the label
  setTimeout(() => { btn._busy = false }, 2000)
}
async function testConn() {
  const btn = $('btn-test')
  btn.textContent = '⏳'
  btn.disabled    = true
  try {
    const r = await fetch('/api/test')
    const j = await r.json()
    if (j.ok) {
      showError('')
      const mode = j.paper ? 'PAPER' : 'LIVE'
      alert(`✓ Connected [${mode}]\nEquity: $${parseFloat(j.equity).toLocaleString()}\nCash:   $${parseFloat(j.cash).toLocaleString()}\nStatus: ${j.status}`)
    } else {
      showError('Connection failed: ' + j.error)
    }
  } catch(e) {
    showError('Test request failed: ' + e.message)
  } finally {
    btn.textContent = '🔌 Test'
    btn.disabled    = false
  }
}
function showError(msg) {
  const eb = $('error-bar')
  if (!eb) return
  if (!msg) { eb.style.display = 'none'; return }
  eb.style.display = 'block'
  eb.textContent   = '⚠ ' + msg
  eb.scrollIntoView({behavior:'smooth', block:'nearest'})
}
async function closePos(ticker) {
  if (!confirm(`Close ${ticker}?`)) return
  const r = await fetch(`/api/close/${ticker}`, {method:'POST'})
  const j = await r.json()
  if (!j.ok) alert('Close failed — check log')
}

// ── Gear Drawer ──────────────────────────────────
function toggleConfig() {
  const drawer   = $('cfg-drawer')
  const backdrop = $('cfg-backdrop')
  if (!drawer) return
  const opening = !drawer.classList.contains('open')
  drawer.classList.toggle('open', opening)
  if (backdrop) backdrop.classList.toggle('open', opening)
}

// ── Left-column tab switcher ─────────────────────
function switchTab(name) {
  ['wl','tr','an'].forEach(t => {
    const pane = $('tab-pane-' + t)
    const btn  = $('tab-btn-'  + t)
    if (pane) pane.classList.toggle('active', name === t)
    if (btn)  btn.classList.toggle('active',  name === t)
  })
  if (name === 'an') loadAnalytics()
}

// ── Analytics tab ─────────────────────────────────
let _anChart = null   // Chart.js instance (destroyed on reload)
let _anLoading = false

async function loadAnalytics() {
  if (_anLoading) return
  _anLoading = true
  const days = ($('an-days') && $('an-days').value) || 90
  try {
    const r = await fetch('/api/analytics?days=' + days)
    const d = await r.json()
    renderAnalytics(d)
  } catch(e) {
    const cards = $('an-cards')
    if (cards) cards.innerHTML = '<div class="an-empty">Could not load analytics: ' + e.message + '</div>'
  } finally {
    _anLoading = false
  }
}

function renderAnalytics(d) {
  const s      = d.stats || {}
  const cards  = $('an-cards')
  const eq_pts = d.equity || []

  if (!cards) return

  if (!s.total) {
    cards.innerHTML = '<div class="an-empty">No trade history yet &mdash; run the bot to build data.</div>'
    ;['an-chart-wrap','an-ticker-wrap','an-trades-wrap'].forEach(id => {
      const el = $(id); if (el) el.style.display = 'none'
    })
    return
  }

  // ── Stat cards ──────────────────────────────────
  const wrColor  = s.win_rate  >= 50 ? 'var(--green)' : 'var(--red)'
  const expColor = s.expectancy >= 0 ? 'var(--green)' : 'var(--red)'
  const pnlColor = s.total_pnl  >= 0 ? 'var(--green)' : 'var(--red)'
  cards.innerHTML = `
    <div class="an-card">
      <div class="an-card-label">Trades</div>
      <div class="an-card-value">${s.total}</div>
    </div>
    <div class="an-card">
      <div class="an-card-label">Win Rate</div>
      <div class="an-card-value" style="color:${wrColor}">${s.win_rate}%</div>
    </div>
    <div class="an-card">
      <div class="an-card-label">Avg Winner</div>
      <div class="an-card-value" style="color:var(--green)">${s.avg_win > 0 ? '+' : ''}${s.avg_win}%</div>
    </div>
    <div class="an-card">
      <div class="an-card-label">Avg Loser</div>
      <div class="an-card-value" style="color:var(--red)">${s.avg_loss}%</div>
    </div>
    <div class="an-card">
      <div class="an-card-label">Expectancy</div>
      <div class="an-card-value" style="color:${expColor}">${s.expectancy > 0 ? '+' : ''}${s.expectancy}%</div>
    </div>
    <div class="an-card">
      <div class="an-card-label">Total P&L</div>
      <div class="an-card-value" style="color:${pnlColor}">${s.total_pnl > 0 ? '+' : ''}${s.total_pnl}%</div>
    </div>
    <div class="an-card">
      <div class="an-card-label">Max DD</div>
      <div class="an-card-value" style="color:var(--red)">-${s.max_drawdown}%</div>
    </div>`

  // ── Equity curve ─────────────────────────────────
  const chartWrap = $('an-chart-wrap')
  if (chartWrap && eq_pts.length > 1) {
    chartWrap.style.display = 'block'
    const canvas = $('an-equity-chart')
    if (_anChart) { _anChart.destroy(); _anChart = null }
    const finalEq = eq_pts[eq_pts.length - 1].equity
    const lineColor = finalEq >= 1.0 ? '#00c853' : '#ff5252'
    _anChart = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels: eq_pts.map(p => p.date),
        datasets: [{
          data: eq_pts.map(p => p.equity),
          borderColor: lineColor,
          borderWidth: 2,
          pointRadius: 0,
          fill: true,
          backgroundColor: finalEq >= 1.0 ? 'rgba(0,200,83,0.08)' : 'rgba(255,82,82,0.08)',
          tension: 0.2
        }]
      },
      options: {
        animation: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { display: false },
          y: {
            grid: { color: 'rgba(48,54,61,0.5)' },
            ticks: { color: 'var(--muted)', font: { size: 10 } }
          }
        }
      }
    })
  }

  // ── Per-ticker table ──────────────────────────────
  const tickerWrap = $('an-ticker-wrap')
  const tickerBody = $('an-ticker-body')
  if (tickerWrap && tickerBody && d.by_ticker) {
    tickerWrap.style.display = 'block'
    const tickers = Object.entries(d.by_ticker)
      .sort((a,b) => b[1].total_pnl - a[1].total_pnl)
    tickerBody.innerHTML = tickers.map(([tk, v]) => {
      const pnlColor = v.total_pnl >= 0 ? 'var(--green)' : 'var(--red)'
      const wrColor  = v.win_rate  >= 50 ? 'var(--green)' : 'var(--red)'
      return `<tr>
        <td><strong>${tk}</strong></td>
        <td>${v.trades}</td>
        <td style="color:${wrColor}">${v.win_rate}%</td>
        <td style="color:${v.avg_pnl>=0?'var(--green)':'var(--red)'}">
          ${v.avg_pnl > 0 ? '+' : ''}${v.avg_pnl}%</td>
        <td style="color:${pnlColor}">${v.total_pnl > 0 ? '+' : ''}${v.total_pnl}%</td>
      </tr>`
    }).join('')
  }

  // ── Recent exits table ────────────────────────────
  const tradesWrap = $('an-trades-wrap')
  const tradesBody = $('an-trades-body')
  if (tradesWrap && tradesBody && d.trades) {
    tradesWrap.style.display = 'block'
    const recent = [...d.trades].reverse().slice(0, 40)
    tradesBody.innerHTML = recent.map(t => {
      const pnlColor = t.pnl_pct >= 0 ? 'var(--green)' : 'var(--red)'
      const floatM   = t.float_m ? parseFloat(t.float_m).toFixed(1) + 'M' : '?'
      const isMicro  = t.float_m && parseFloat(t.float_m) <= 10
      const floatBadge = t.float_m
        ? `<span class="float-badge ${isMicro?'micro':'large'}">${isMicro?'⚡ ':''}${floatM}</span>`
        : '—'
      return `<tr>
        <td>${t.date}</td>
        <td><strong>${t.ticker}</strong></td>
        <td style="color:var(--muted);font-size:10px">${t.action}</td>
        <td style="color:${pnlColor};font-family:var(--mono)">
          ${t.pnl_pct > 0 ? '+' : ''}${t.pnl_pct}%</td>
        <td style="font-family:var(--mono)">${t.streak || '—'}</td>
        <td>${floatBadge}</td>
      </tr>`
    }).join('')
  }
}

// ── Trades card toggle ────────────────────────────
function toggleTrades() {
  const body = $('trades-collapsed-body')
  const arr  = $('trades-arrow')
  if (!body) return
  const hidden = body.style.display === 'none'
  body.style.display = hidden ? '' : 'none'
  if (arr) arr.textContent = hidden ? '▲' : '▼'
}

function setMode(m) {
  currentMode = m
  updateModeBtns()
}

function updateModeBtns() {
  $('btn-paper').className = 'mode-btn' + (currentMode==='paper' ? ' active-paper' : '')
  $('btn-live').className  = 'mode-btn' + (currentMode==='live'  ? ' active-live'  : '')
}

function populateCfg(cfg) {
  if (cfg.api_key)    $('cfg-api-key').value    = cfg.api_key
  if (cfg.secret_key) $('cfg-secret-key').value = cfg.secret_key
  $('cfg-stop').value     = ((cfg.stop_loss_pct    || 0.04)  * 100).toFixed(1)
  $('cfg-tp').value       = ((cfg.take_profit_pct  || 0.12)  * 100).toFixed(1)
  $('cfg-pos-size').value = ((cfg.position_size_pct|| 0.10)  * 100).toFixed(0)
  $('cfg-trail-act').value= ((cfg.trail_activation_pct||0.05)* 100).toFixed(1)
  $('cfg-trail-pct').value= ((cfg.trail_stop_pct   || 0.04)  * 100).toFixed(1)
  $('cfg-interval').value = cfg.scan_interval_sec || 60
  $('cfg-bar').value      = cfg.bar_timeframe || '1Min'
  $('cfg-pyramid').value  = cfg.pyramid_enabled ? 'true' : 'false'
  $('cfg-pyr-gain').value = ((cfg.pyramid_gain_pct || 0.05) * 100).toFixed(1)
  $('cfg-trend-price').value = (cfg.trending_max_price || 5.0).toFixed(2)
  $('cfg-partial-on').value  = cfg.partial_exit_enabled !== false ? 'true' : 'false'
  $('cfg-partial-pct').value = ((cfg.partial_exit_pct  || 0.06) * 100).toFixed(1)
  $('cfg-partial-qty').value = ((cfg.partial_exit_qty_pct || 0.50) * 100).toFixed(0)
  $('cfg-float-on').value    = cfg.use_float_filter !== false ? 'true' : 'false'
  $('cfg-float-max').value   = cfg.max_float_million || 50
  $('cfg-micro-float').value = cfg.micro_float_threshold || 10
  $('cfg-rvol-on').value   = cfg.use_rvol !== false ? 'true' : 'false'
  $('cfg-rvol-min').value  = cfg.min_rvol || 2.0
  $('cfg-auto-sched').value = cfg.auto_schedule !== false ? 'true' : 'false'
  const openAt = cfg.market_open_at || [9,15]
  $('cfg-open-h').value = openAt[0]
  $('cfg-open-m').value = String(openAt[1]).padStart(2,'0')
  const eodAt  = cfg.eod_liquidate_at || [16,0]
  const pad = n => String(n).padStart(2,'0')
  $('sched-status').textContent = cfg.auto_schedule !== false
    ? `⏰ Bot will auto-start at ${pad(openAt[0])}:${pad(openAt[1])} ET and liquidate at ${pad(eodAt[0])}:${pad(eodAt[1])} ET on weekdays`
    : '⏸ Auto-schedule disabled — start manually'
  // ── Strategy ──────────────────────────────────────
  $('cfg-strategy').value       = cfg.strategy || 'exhaustion'
  $('cfg-rte-side').value       = cfg.rte_side || 'red'
  $('cfg-rte-threshold').value  = cfg.rte_threshold  != null ? cfg.rte_threshold  : 20
  $('cfg-rte-avg-ma').value     = cfg.rte_avg_ma     != null ? cfg.rte_avg_ma     : 3
  $('cfg-rte-boxes').value      = cfg.rte_min_boxes  != null ? cfg.rte_min_boxes  : 3
  $('cfg-cm-rsi-thresh').value  = cfg.rmi_oversold != null ? cfg.rmi_oversold : 10
  $('cfg-use-rte').value        = cfg.use_rte_exhaustion !== false ? 'true' : 'false'
  $('cfg-use-cm-rsi').value     = cfg.use_rmi            !== false ? 'true' : 'false'
  $('cfg-use-vol-trend').value  = cfg.use_volume_trending_up !== false ? 'true' : 'false'
  $('cfg-use-macd').value       = cfg.use_macd              !== false ? 'true' : 'false'
  $('cfg-macd-fast').value      = cfg.macd_fast   || 12
  $('cfg-macd-slow').value      = cfg.macd_slow   || 26
  $('cfg-macd-signal').value    = cfg.macd_signal || 9
  $('cfg-tv-url').value         = cfg.tv_chart_url || 'https://www.tradingview.com/chart/x04Gfcu8/'
  _tvChartUrl = cfg.tv_chart_url || 'https://www.tradingview.com/chart/x04Gfcu8/'
  $('cfg-debug-signals').value  = cfg.debug_signals ? 'true' : 'false'
  // ── Pre-Market ─────────────────────────────────────
  $('cfg-pm-enabled').value = cfg.pre_market_enabled ? 'true' : 'false'
  const pmStart = cfg.pre_market_start || [4, 0]
  $('cfg-pm-h').value = pmStart[0]
  $('cfg-pm-m').value = String(pmStart[1]).padStart(2,'0')
  $('cfg-pm-offset').value = ((cfg.pre_market_limit_offset_pct != null ? cfg.pre_market_limit_offset_pct : 0.002) * 100).toFixed(2)
  $('cfg-pm-vol-mult').value = cfg.pre_market_volume_surge_mult != null ? cfg.pre_market_volume_surge_mult : 0.3
  // ── Capital Protection ─────────────────────────────
  $('cfg-max-daily-loss').value = ((cfg.max_daily_loss_pct != null ? cfg.max_daily_loss_pct : 0.05) * 100).toFixed(1)
  $('cfg-vol-surge').value      = cfg.volume_surge_mult != null ? cfg.volume_surge_mult : 1.5
  $('cfg-bar-count').value      = cfg.bar_count || 800
  onStrategyChange()
  currentMode = cfg.paper ? 'paper' : 'live'
  // Populate watchlist chips — skip if user has unsaved edits (SSE must not overwrite pending changes)
  if (!watchlistEdited) initWatchlistChips(cfg.tickers || [])
  updateModeBtns()
}

async function saveConfig() {
  const tvUrl = ($('cfg-tv-url').value || '').trim()
  if (tvUrl) _tvChartUrl = tvUrl
  const payload = {
    paper:              currentMode === 'paper',
    api_key:            $('cfg-api-key').value.trim(),
    secret_key:         $('cfg-secret-key').value.trim(),
    stop_loss_pct:      parseFloat($('cfg-stop').value) / 100,
    take_profit_pct:    parseFloat($('cfg-tp').value) / 100,
    position_size_pct:  parseFloat($('cfg-pos-size').value) / 100,
    trail_activation_pct: parseFloat($('cfg-trail-act').value) / 100,
    trail_stop_pct:     parseFloat($('cfg-trail-pct').value) / 100,
    scan_interval_sec:  parseInt($('cfg-interval').value),
    bar_timeframe:      $('cfg-bar').value,
    pyramid_enabled:    $('cfg-pyramid').value === 'true',
    pyramid_gain_pct:   parseFloat($('cfg-pyr-gain').value) / 100,
    trending_max_price:    parseFloat($('cfg-trend-price').value) || 5.0,
    partial_exit_enabled:  $('cfg-partial-on').value === 'true',
    partial_exit_pct:      parseFloat($('cfg-partial-pct').value) / 100,
    partial_exit_qty_pct:  parseFloat($('cfg-partial-qty').value) / 100,
    use_float_filter:        $('cfg-float-on').value === 'true',
    max_float_million:       parseFloat($('cfg-float-max').value) || 50,
    micro_float_threshold:   parseFloat($('cfg-micro-float').value) || 10,
    use_rvol:           $('cfg-rvol-on').value === 'true',
    min_rvol:           parseFloat($('cfg-rvol-min').value) || 2.0,
    auto_schedule:      $('cfg-auto-sched').value === 'true',
    market_open_at:     [parseInt($('cfg-open-h').value)||9, parseInt($('cfg-open-m').value)||15],
    // ── Strategy ──────────────────────────────────────
    strategy:           $('cfg-strategy').value,
    rte_side:           $('cfg-rte-side').value,
    rte_threshold:      parseInt($('cfg-rte-threshold').value) || 20,
    rte_avg_ma:         parseInt($('cfg-rte-avg-ma').value)    || 3,
    rte_min_boxes:      parseInt($('cfg-rte-boxes').value)     || 3,
    rmi_oversold:       parseInt($('cfg-cm-rsi-thresh').value) || 10,
    use_rte_exhaustion:     $('cfg-use-rte').value === 'true',
    use_rmi:                $('cfg-use-cm-rsi').value === 'true',
    use_volume_trending_up: $('cfg-use-vol-trend').value === 'true',
    use_macd:               $('cfg-use-macd').value === 'true',
    macd_fast:          parseInt($('cfg-macd-fast').value) || 12,
    macd_slow:          parseInt($('cfg-macd-slow').value) || 26,
    macd_signal:        parseInt($('cfg-macd-signal').value) || 9,
    tv_chart_url:       tvUrl || 'https://www.tradingview.com/chart/x04Gfcu8/',
    debug_signals:      $('cfg-debug-signals').value === 'true',
    pre_market_enabled: $('cfg-pm-enabled').value === 'true',
    pre_market_start:   [parseInt($('cfg-pm-h').value)||4, parseInt($('cfg-pm-m').value)||0],
    pre_market_limit_offset_pct: parseFloat($('cfg-pm-offset').value) / 100 || 0.002,
    pre_market_volume_surge_mult: parseFloat($('cfg-pm-vol-mult').value) || 0.3,
    // ── Capital Protection ─────────────────────────────
    max_daily_loss_pct: parseFloat($('cfg-max-daily-loss').value) / 100 || 0.05,
    volume_surge_mult:  parseFloat($('cfg-vol-surge').value) || 1.5,
    bar_count:          parseInt($('cfg-bar-count').value) || 800,
  }
  const r = await fetch('/api/config', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)})
  const j = await r.json()
  const msg = $('cfg-save-msg')
  msg.textContent = j.ok ? '✓ Saved' : '✗ Error'
  msg.style.color = j.ok ? 'var(--green)' : 'var(--red)'
  setTimeout(() => { msg.textContent = '' }, 3000)
}

// ── Locked tickers ───────────────────────────────
function renderLockedTickers(tickers) {
  const section = $('locked-section')
  const chips   = $('locked-chips')
  if (!section || !chips) return
  if (!tickers.length) {
    section.style.display = 'none'
    return
  }
  section.style.display = 'block'
  chips.innerHTML = tickers.map(t =>
    `<span class="lock-chip" onclick="unlockTicker('${t}')" title="Click to re-enable auto-buy for ${t}">
       🔒 ${t} <span style="font-size:10px;color:var(--muted)">unlock</span>
     </span>`
  ).join('')
}

async function unlockTicker(ticker) {
  if (!confirm(`Re-enable auto-buy for ${ticker}?\\nThe bot will buy it again if a signal fires.`)) return
  const r = await fetch(`/api/unlock/${ticker}`, {method:'POST'})
  const j = await r.json()
  if (!j.ok) alert('Unlock failed')
}

// ── Strategy section visibility ───────────────────
function onStrategyChange() {
  const strat = $('cfg-strategy').value
  const exhEl = $('strat-exhaustion-section')
  const macdEl = $('strat-macd-section')
  if (!exhEl || !macdEl) return
  // Exhaustion section: show for "exhaustion" only
  exhEl.style.display  = strat === 'exhaustion' ? '' : 'none'
  // MACD section: show for "exhaustion" (MACD is a component) and "macd"
  macdEl.style.display = (strat === 'exhaustion' || strat === 'macd') ? '' : 'none'
}

// ── Helpers ──────────────────────────────────────
function fmtNum(n) {
  return Number(n).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})
}
function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
}

// ── Watchlist Manager ────────────────────────────
let wlOpen = false
let watchlistSet = new Set()
// Core tickers — always present, cannot be removed from the UI
const CORE_TICKERS = new Set(['SQQQ', 'UVXY'])

function renderWlBannerChips() {
  const el = $('wl-banner-chips')
  if (!el) return
  if (wlOpen) { el.innerHTML = ''; return }  // clear when expanded — full chips visible below
  const sorted = [...watchlistSet].sort()
  el.innerHTML = sorted.map(t => {
    const dot = srcDotHtml(t)
    return `<a class="wl-bc" href="${tvUrl(t)}" target="_blank"
               onmouseenter="showTip(this,tipForTicker('${t}'))" onmouseleave="hideTip()">
               ${dot}${t}</a>`
  }).join('')
}

function toggleWatchlist() {
  // In new layout watchlist is always visible in the left tab — no-op for collapse
  switchTab('wl')
}

function openWatchlist() {
  switchTab('wl')
}

function initWatchlistChips(tickers) {
  watchlistSet = new Set(tickers.map(t => t.trim().toUpperCase()).filter(Boolean))
  renderWatchlistChips()
  renderWlBannerChips()
}

function renderWatchlistChips() {
  // Highlight Save button when there are unsaved changes
  const saveBtn = $('wl-save-btn')
  if (saveBtn) {
    if (watchlistEdited) {
      saveBtn.textContent = '💾 Save *'
      saveBtn.style.outline = '2px solid var(--yellow)'
    } else {
      saveBtn.textContent = '💾 Save'
      saveBtn.style.outline = ''
    }
  }
  const container = $('wl-chips')
  const filter    = ($('wl-search').value || '').toUpperCase().trim()
  const sorted    = [...watchlistSet].sort()
  if (!sorted.length) {
    container.innerHTML = '<span style="color:var(--muted);font-size:12px">No tickers — add some above.</span>'
  } else {
    container.innerHTML = sorted.map(t => {
      const hidden = filter && !t.includes(filter) ? ' hidden' : ''
      const dot    = srcDotHtml(t)
      return `<span class="wl-chip${hidden}" id="chip-${t}" data-t="${t}">
        <a class="chip-lbl chip-tv" href="${tvUrl(t)}" target="_blank"
          style="display:inline-flex;align-items:center;gap:4px"
          onmouseenter="showTip(this,tipForTicker('${t}'))" onmouseleave="hideTip()">${dot}${t}</a>
        <span class="ic" onclick="testSignal('${t}')"
          onmouseenter="showTip(this,'Test ${t}')" onmouseleave="hideTip()">🔍</span>
        ${CORE_TICKERS.has(t) ? `
        <span class="ic" style="opacity:0.45;cursor:default"
          onmouseenter="showTip(this,'Core strategy ticker — cannot be removed')" onmouseleave="hideTip()">🔒</span>
        ` : `
        <span class="ic" onclick="editTicker('${t}')"
          onmouseenter="showTip(this,'Edit ${t}')" onmouseleave="hideTip()">✎</span>
        <span class="ic rm" onclick="removeTicker('${t}')"
          onmouseenter="showTip(this,'Remove ${t}')" onmouseleave="hideTip()">✕</span>
        `}
      </span>`
    }).join('')
  renderWlBannerChips()
  }
  const visible = filter ? sorted.filter(t => t.includes(filter)).length : sorted.length
  $('wl-count').textContent        = sorted.length
  $('wl-footer-count').textContent = filter
    ? `${visible} shown of ${sorted.length} tickers`
    : `${sorted.length} tickers`
}

function addTicker() {
  const inp = $('wl-add-input')
  const val = inp.value.trim().toUpperCase().replace(/[^A-Z0-9.]/g,'')
  if (!val) return
  watchlistEdited = true
  watchlistSet.add(val)
  inp.value = ''
  renderWatchlistChips()
}

function removeTicker(t) {
  if (CORE_TICKERS.has(t)) {
    alert(`${t} is a core strategy ticker and cannot be removed.\nEdit bot_config.json directly to change core tickers.`)
    return
  }
  watchlistEdited = true
  watchlistSet.delete(t)
  renderWatchlistChips()
}

function editTicker(t) {
  const chip = document.getElementById('chip-' + t)
  if (!chip) return
  chip.classList.add('editing')
  chip.innerHTML = `
    <input class="chip-edit-inp" id="edit-inp-${t}" value="${t}" maxlength="10"
      onkeydown="if(event.key==='Enter')confirmEdit('${t}');else if(event.key==='Escape')cancelEdit('${t}')">
    <span class="ic" onclick="confirmEdit('${t}')" title="Confirm" style="color:var(--yellow)">✓</span>
    <span class="ic rm" onclick="cancelEdit('${t}')" title="Cancel">✕</span>`
  const inp = document.getElementById('edit-inp-' + t)
  if (inp) { inp.focus(); inp.select() }
}

function confirmEdit(old) {
  const inp = document.getElementById('edit-inp-' + old)
  if (!inp) return
  const nw = inp.value.trim().toUpperCase().replace(/[^A-Z0-9.]/g,'')
  if (!nw) { cancelEdit(old); return }
  watchlistEdited = true
  watchlistSet.delete(old)
  watchlistSet.add(nw)
  renderWatchlistChips()
}

function cancelEdit(t) { renderWatchlistChips() }

function filterChips() { renderWatchlistChips() }

function clearAllTickers() {
  if (!confirm('Remove ALL tickers from the watchlist?')) return
  watchlistSet.clear()
  renderWatchlistChips()
}

function bulkImport() {
  const raw = $('wl-paste-area').value
  const parsed = raw.split(/[\s,]+/).map(t => t.trim().toUpperCase().replace(/[^A-Z0-9.]/g,'')).filter(t => t.length >= 1 && t.length <= 8)
  if (!parsed.length) { alert('No valid tickers found in the pasted text.'); return }
  parsed.forEach(t => watchlistSet.add(t))
  $('wl-paste-area').value = ''
  renderWatchlistChips()
  const msg = $('wl-save-msg')
  msg.textContent = `✓ Imported ${parsed.length} ticker${parsed.length!==1?'s':''}`
  msg.style.color = 'var(--accent)'
  setTimeout(() => { msg.textContent = '' }, 3000)
}

// ── Test Strategy on a Ticker ────────────────────
async function testSignal(ticker) {
  // Remove any existing popup
  const old = document.getElementById('signal-popup')
  if (old) old.remove()

  // Create popup
  const popup = document.createElement('div')
  popup.id = 'signal-popup'
  popup.style.cssText = `
    position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);
    background:var(--card2);border:2px solid var(--accent);border-radius:12px;
    padding:20px 24px;min-width:340px;max-width:480px;z-index:9999;
    box-shadow:0 8px 32px rgba(0,0,0,.6);font-family:var(--mono);font-size:13px;
    color:var(--text);
  `
  popup.innerHTML = `<div style="text-align:center;color:var(--accent);font-size:15px;font-weight:700;margin-bottom:12px">
    🔍 Testing ${ticker}…</div>
    <div style="text-align:center;color:var(--muted)">Fetching bars &amp; computing indicators…</div>`
  document.body.appendChild(popup)

  try {
    const r = await fetch('/api/check_signal/' + encodeURIComponent(ticker))
    const d = await r.json()
    if (!d.ok) {
      popup.innerHTML = `<div style="color:var(--red);font-weight:700;margin-bottom:8px">Error: ${d.error}</div>
        <button onclick="this.parentElement.remove()" class="btn btn-muted btn-sm">Close</button>`
      return
    }

    const P = v => v ? '<span style="color:var(--green)">PASS</span>' : '<span style="color:var(--red)">FAIL</span>'
    const sig = d.signal === 'BUY'
      ? '<span style="color:var(--green);font-size:18px;font-weight:700">BUY</span>'
      : d.signal === 'SELL'
        ? '<span style="color:var(--red);font-size:18px;font-weight:700">SELL</span>'
        : '<span style="color:var(--muted);font-size:18px;font-weight:700">HOLD</span>'

    let body = `
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
        <div>
          <a href="${tvUrl(ticker)}" target="_blank" style="color:var(--accent);font-size:18px;font-weight:700;text-decoration:none">${d.ticker}</a>
          <span style="color:var(--muted);font-size:12px;margin-left:6px">$${d.close}  (${d.bars} bars)</span>
        </div>
        <div>${sig}</div>
      </div>
      <div style="font-size:11px;color:var(--muted);margin-bottom:8px">Strategy: ${d.strategy}</div>
    `

    if (d.strategy === 'exhaustion') {
      const boxPass = d.rte_boxes_streak >= d.rte_min_boxes
      body += `<table style="width:100%;font-size:12px;border-collapse:collapse">
        <tr><td style="padding:3px 6px;border-bottom:1px solid var(--border)">%R Extreme Zone</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${P(d.rte_extreme)}</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${d.rte_extreme ? 'In zone' : 'Not in zone'}</td></tr>
        <tr><td style="padding:3px 6px;border-bottom:1px solid var(--border)">%R Reversal (exit zone)</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${P(d.rte_reversal)}</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${d.rte_reversal ? 'Signal bar!' : 'No'}</td></tr>
        <tr><td style="padding:3px 6px;border-bottom:1px solid var(--border)">Box Streak / Min</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${P(boxPass)}</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${d.rte_boxes_streak} consecutive  (need ≥${d.rte_min_boxes}, ${d.rte_boxes_total} total)</td></tr>
        <tr><td style="padding:3px 6px;border-bottom:1px solid var(--border)">RSI-2 Signal</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${P(d.rsi2_pass)}</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${d.rsi2} (need &lt;${d.rsi2_threshold}, above SMA200, below SMA5)</td></tr>
        <tr><td style="padding:3px 6px;border-bottom:1px solid var(--border)">Volume Trending Up</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${P(d.vol_trend_up)}</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${d.vol_trend_up ? 'Yes' : 'No'}</td></tr>
        <tr><td style="padding:3px 6px;border-bottom:1px solid var(--border)">MACD Bullish Cross</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${P(d.macd_bull)}</td>
            <td style="padding:3px 6px;border-bottom:1px solid var(--border)">${d.macd_bull ? 'Yes' : 'No'}</td></tr>
        <tr><td style="padding:3px 6px">RSI</td>
            <td style="padding:3px 6px" colspan="2">${d.rsi}</td></tr>
      </table>`
    }

    body += `<div style="margin-top:14px;text-align:right">
      <a href="${tvUrl(ticker)}" target="_blank" class="btn btn-sm" style="margin-right:8px;color:var(--accent);text-decoration:none">Open TradingView</a>
      <button onclick="this.closest('#signal-popup').remove()" class="btn btn-muted btn-sm">Close</button>
    </div>`

    popup.innerHTML = body
  } catch(e) {
    popup.innerHTML = `<div style="color:var(--red)">Request failed: ${e.message}</div>
      <button onclick="this.parentElement.remove()" class="btn btn-muted btn-sm" style="margin-top:8px">Close</button>`
  }
}

async function openAllInTV() {
  const tickers = [...watchlistSet].sort()
  if (!tickers.length) {
    alert('Watchlist is empty — add some tickers first.')
    return
  }

  // TradingView watchlist import format: one ticker per line
  const tvText = tickers.join('\\n')

  // Copy to clipboard
  let copied = false
  try {
    await navigator.clipboard.writeText(tvText)
    copied = true
  } catch(e) {
    // Fallback for browsers that block clipboard without user gesture
    const ta = document.createElement('textarea')
    ta.value = tvText
    ta.style.position = 'fixed'; ta.style.opacity = '0'
    document.body.appendChild(ta); ta.select()
    try { copied = document.execCommand('copy') } catch(_) {}
    document.body.removeChild(ta)
  }

  // Open TradingView — first ticker on the saved chart, rest in new tabs
  const base = (_tvChartUrl || 'https://www.tradingview.com/chart/x04Gfcu8/').replace(/\/?$/, '/')

  if (tickers.length === 1) {
    window.open(base + '?symbol=' + encodeURIComponent(tickers[0]), '_blank')
  } else {
    // Open first ticker on the saved chart
    window.open(base + '?symbol=' + encodeURIComponent(tickers[0]), '_blank')
    // Show instructions to import the rest
    const msg = $('wl-save-msg')
    msg.textContent = copied
      ? `✓ Copied ${tickers.length} tickers — paste into TradingView Watchlist → ⋮ → Import symbols`
      : `⚠ Clipboard blocked — open TradingView and manually add: ${tickers.slice(0,5).join(', ')}…`
    msg.style.color = copied ? 'var(--accent)' : 'var(--yellow)'
    setTimeout(() => { msg.textContent = '' }, 7000)
    return
  }

  if (copied) {
    const msg = $('wl-save-msg')
    msg.textContent = '✓ Tickers copied — paste into TradingView Watchlist → ⋮ → Import symbols'
    msg.style.color = 'var(--accent)'
    setTimeout(() => { msg.textContent = '' }, 6000)
  }
}

async function saveWatchlist() {
  // Always ensure core tickers are present before saving
  CORE_TICKERS.forEach(t => watchlistSet.add(t))
  const tickers = [...watchlistSet].sort()
  const r = await fetch('/api/config', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ tickers })
  })
  const j = await r.json()
  const msg = $('wl-save-msg')
  if (j.ok) {
    watchlistEdited = false   // clear pending-edit flag — SSE can sync again
    msg.textContent = `✓ Saved ${tickers.length} tickers`
    msg.style.color = 'var(--green)'
  } else {
    msg.textContent = '✗ Error saving'
    msg.style.color = 'var(--red)'
  }
  setTimeout(() => { msg.textContent = '' }, 3000)
}

// ── Trending ─────────────────────────────────────
let lastTrendingList    = []
let lastTrendingSources = {}          // {TICKER: "stocktwits"|"finviz"|"both"}
let lastTrendingPrices  = {}          // {TICKER: price float}
let dismissedTrending   = new Set()   // hidden until next refresh

// ── Tooltip engine ───────────────────────────────
const SOURCE_COLOR = { stocktwits:'#e86900', finviz:'#1a6bff', both:'#8b5cf6' }
const SOURCE_LABEL = { stocktwits:'StockTwits', finviz:'Finviz', both:'ST + Finviz' }

function srcColor(ticker) {
  const s = lastTrendingSources[ticker] || ''
  return SOURCE_COLOR[s] || null
}
function srcDotHtml(ticker) {
  const c = srcColor(ticker)
  if (!c) return ''
  return `<span class="src-dot" style="background:${c}"></span>`
}
function tipForTicker(t) {
  const src   = lastTrendingSources[t] || ''
  const price = lastTrendingPrices[t]
  const col   = SOURCE_COLOR[src] || 'var(--muted)'
  const lbl   = SOURCE_LABEL[src] || ''
  let h = `<strong style="color:#fff;font-family:var(--mono)">${t}</strong>`
  if (price) h += `<span style="color:#6b7ea8;margin-left:10px">$${price}</span>`
  if (lbl)   h += `<br><span style="color:${col}">● ${lbl}</span>`
  return h
}
function showTip(el, html) {
  const tt = document.getElementById('tt')
  tt.innerHTML = html
  tt.style.display = 'block'
  const r  = el.getBoundingClientRect()
  const tw = tt.offsetWidth
  const th = tt.offsetHeight
  let left = r.left + r.width / 2 - tw / 2
  let top  = r.top - th - 10
  // Clamp within viewport
  left = Math.max(8, Math.min(left, window.innerWidth  - tw - 8))
  if (top < 8) top = r.bottom + 10   // flip below if no room above
  tt.style.left = left + 'px'
  tt.style.top  = top  + 'px'
}
function hideTip() { document.getElementById('tt').style.display = 'none' }

function renderTrendingChips(tickers, updatedAt, sources) {
  if (sources) lastTrendingSources = sources
  const chips = $('trend-chips')
  const tsEl  = $('trend-updated')
  const visible = (tickers || []).filter(t => !dismissedTrending.has(t))
  if (!visible.length) {
    chips.innerHTML = '<span style="color:var(--muted);font-size:12px">No under-$5 trending stocks found</span>'
  } else {
    chips.innerHTML = visible.map(t => {
      const inWl = watchlistSet.has(t)
      const dot  = srcDotHtml(t)
      return `<span class="tr-chip" id="tr-${t}">
        <a class="tr-tv" href="${tvUrl(t)}" target="_blank"
          onmouseenter="showTip(this,tipForTicker('${t}'))" onmouseleave="hideTip()"
          style="display:inline-flex;align-items:center;gap:4px;color:var(--accent);
                 text-decoration:none;padding:3px 4px 3px 8px;font-weight:600">
          ${dot}${t}</a>
        <span class="tr-test" onclick="testSignal('${t}')"
          onmouseenter="showTip(this,'Test strategy on ${t}')" onmouseleave="hideTip()"
          style="cursor:pointer;padding:3px 4px;color:var(--muted);font-size:12px">🔍</span>
        <span class="tr-add" onclick="addTickerFromTrend('${t}')"
          onmouseenter="showTip(this,'${inWl ? 'Already in watchlist' : 'Add '+t+' to watchlist'}')"
          onmouseleave="hideTip()"
          style="${inWl ? 'color:var(--green)' : ''}">${inWl ? '✓' : '+'}</span>
        <span class="tr-sep"></span>
        <span class="tr-rm" onclick="dismissTrending('${t}')"
          onmouseenter="showTip(this,'Dismiss ${t}')" onmouseleave="hideTip()">✕</span>
      </span>`
    }).join('')
  }
  if (updatedAt) tsEl.textContent = 'Updated ' + updatedAt
}

function dismissTrending(t) {
  dismissedTrending.add(t)
  renderTrendingChips(lastTrendingList, null, null)
}

function addTickerFromTrend(t) {
  watchlistEdited = true
  watchlistSet.add(t.toUpperCase())
  renderWatchlistChips()
  renderTrendingChips(lastTrendingList, null, null)   // re-render so chip shows ✓
  const msg = $('wl-save-msg')
  if ($('wl-body').classList.contains('open')) {
    msg.textContent = `+ ${t} added to watchlist`
    msg.style.color = 'var(--green)'
    setTimeout(() => { msg.textContent = '' }, 2000)
  }
}

async function refreshTrending() {
  const btn = $('trend-btn')
  btn.textContent = '…'
  btn.disabled    = true
  try {
    const r = await fetch('/api/trending')
    const j = await r.json()
    lastTrendingList    = j.tickers  || []
    lastTrendingSources = j.sources  || {}
    lastTrendingPrices  = j.prices   || {}
    dismissedTrending   = new Set()  // fresh list = clear dismissals
    renderTrendingChips(lastTrendingList, j.updated, lastTrendingSources)
  } catch(e) {
    $('trend-updated').textContent = 'Fetch failed'
  } finally {
    btn.textContent = '↻ Refresh'
    btn.disabled    = false
  }
}

// Update trending from SSE data (no extra fetch needed)
function syncTrendingFromState(d) {
  if (d.trending_tickers && d.trending_tickers.length) {
    if (JSON.stringify(d.trending_tickers) !== JSON.stringify(lastTrendingList)) {
      lastTrendingList    = d.trending_tickers
      lastTrendingSources = d.trending_sources || {}
      lastTrendingPrices  = d.trending_prices  || {}
      dismissedTrending   = new Set()   // new data = reset dismissals
      renderTrendingChips(lastTrendingList, d.trending_updated, lastTrendingSources)
      renderWlBannerChips()   // refresh source dots on watchlist banner
    }
  }
}

// Run on page load + every 5 minutes
refreshTrending()
setInterval(refreshTrending, 5 * 60 * 1000)

// Auto-start bot when page loads (SSE will quickly reflect actual state)
toggleBot()

window._dashboardLoaded = true
console.log('[Dashboard] Script fully loaded ✓')
</script>
<div id="tt"></div>
</body>
</html>"""

# ═══════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"""
╔══════════════════════════════════════════╗
║   Alpaca Momentum Bot  —  Dashboard      ║
║   http://localhost:{PORT}                   ║
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
