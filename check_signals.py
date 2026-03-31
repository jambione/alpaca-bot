#!/usr/bin/env python3
"""
check_signals.py — Verify indicator values against TradingView

Usage:
    python check_signals.py TICKER [--bars 20] [--tf 5Min]

Example:
    python check_signals.py TZA
    python check_signals.py TSLA --bars 30 --tf 15Min

Prints the last N bars with every indicator value the bot computes,
so you can line them up against TradingView side-by-side.
"""

import argparse, json, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── Load config for API keys ──────────────────────────────────────────────────
CFG_FILE = Path(__file__).parent / "bot_config.json"
if not CFG_FILE.exists():
    print("ERROR: bot_config.json not found — run the dashboard first to generate it.")
    sys.exit(1)

with open(CFG_FILE) as f:
    cfg = json.load(f)

API_KEY    = cfg.get("api_key", "")
SECRET_KEY = cfg.get("secret_key", "")
PAPER      = cfg.get("paper", True)

if not API_KEY or not SECRET_KEY:
    print("ERROR: api_key / secret_key missing in bot_config.json")
    sys.exit(1)

# ── CLI args ──────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Check bot indicator values for a ticker")
parser.add_argument("ticker",        type=str,          help="Ticker symbol, e.g. TZA")
parser.add_argument("--bars",        type=int,  default=20,    help="Rows to print (default 20)")
parser.add_argument("--tf",          type=str,  default=None,  help="Bar timeframe override, e.g. 1Min, 5Min, 15Min")
parser.add_argument("--bar-count",   type=int,  default=None,  help="Total bars to fetch (default from config)")
args = parser.parse_args()

ticker    = args.ticker.upper()
tf        = args.tf        or cfg.get("bar_timeframe", "5Min")
bar_count = args.bar_count or cfg.get("bar_count", 300)
show_rows = args.bars

# ── Indicator helpers (copied verbatim from alpaca_dashboard.py) ──────────────
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def williams_pr(high, low, close, length):
    hh = high.rolling(length).max()
    ll = low.rolling(length).min()
    return np.where(hh == ll, -50.0, (close - hh) / (hh - ll) * 100)

def compute_percent_r_exhaustion(df, cfg):
    """%R Trend Exhaustion - on deck / off deck logic."""
    s_pr = pd.Series(williams_pr(df["high"], df["low"], df["close"], 21),  index=df.index)
    l_pr = pd.Series(williams_pr(df["high"], df["low"], df["close"], 112), index=df.index)
    s_percentR   = s_pr.ewm(span=7, adjust=False).mean()
    l_percentR   = l_pr.ewm(span=3, adjust=False).mean()
    avg_ma       = cfg.get("rte_avg_ma", 3)
    avg_percentR = (s_percentR + l_percentR) / 2
    final        = avg_percentR.ewm(span=avg_ma, adjust=False).mean()
    threshold    = cfg.get("rte_threshold", 20)
    side         = cfg.get("rte_side", "red").lower()
    
    if side == "red":
        overbought = final >= -threshold
    else:
        overbought = final <= (-100 + threshold)
    
    # ON DECK: consecutive bars in overbought zone (trend established)
    overbought_prev = overbought.shift(1).fillna(False)
    ob_consecutive  = overbought.groupby((~overbought).cumsum()).cumcount() + 1
    ob_consecutive   = np.where(overbought, ob_consecutive, 0)
    min_bars         = cfg.get("rte_min_bars", 2)
    ob_on_deck       = overbought & (ob_consecutive >= min_bars)
    ob_trend_start   = overbought & ~overbought_prev
    
    # OFF DECK: reversal/exit from overbought zone
    ob_reversal = (~overbought) & overbought_prev
    
    df["rte_final"]      = final.round(2)
    df["overbought"]     = overbought
    df["ob_consecutive"] = ob_consecutive
    df["ob_on_deck"]     = ob_on_deck
    df["ob_trend_start"] = ob_trend_start
    df["ob_reversal"]    = ob_reversal  # OFF DECK signal
    return df

def compute_cm_rsi_lower(df, cfg):
    """CM RSI-2: RSI < 25 with price MA conditions (close > SMA200 AND close < SMA5)."""
    delta = df["close"].diff()
    up    = delta.clip(lower=0).ewm(alpha=0.5, adjust=False).mean()
    down  = (-delta.clip(upper=0)).ewm(alpha=0.5, adjust=False).mean()
    rsi2  = np.where(down == 0, 100, np.where(up == 0, 0, 100 - (100 / (1 + up / down))))
    df["cm_rsi"] = pd.Series(rsi2, index=df.index).round(2)
    
    # MA conditions from original Pine script
    df["ma_200"] = df["close"].rolling(200).mean()
    df["ma_5"]   = df["close"].rolling(5).mean()
    df["cm_rsi_approaching"] = (
        (df["close"] > df["ma_200"]) &
        (df["close"] < df["ma_5"]) &
        (df["cm_rsi"] < cfg.get("cm_rsi_approaching", 25))
    )
    return df

def compute_macd(df, cfg):
    fast = cfg.get("macd_fast", 12)
    slow = cfg.get("macd_slow", 26)
    sig  = cfg.get("macd_signal", 9)
    df["macd_line"]        = (ema(df["close"], fast) - ema(df["close"], slow)).round(4)
    df["macd_signal_line"] = ema(df["macd_line"], sig).round(4)
    df["macd_hist"]        = (df["macd_line"] - df["macd_signal_line"]).round(4)
    df["macd_bull"] = (df["macd_line"] > df["macd_signal_line"]) & \
                      (df["macd_line"].shift(1) <= df["macd_signal_line"].shift(1))
    df["macd_bear"] = (df["macd_line"] < df["macd_signal_line"]) & \
                      (df["macd_line"].shift(1) >= df["macd_signal_line"].shift(1))
    return df

def compute_obv_oscillator(df, cfg):
    """OBV Oscillator: OBV - EMA(OBV, 20) above zero AND rising."""
    obv_len = cfg.get("obv_length", 20)
    obv     = (np.sign(df["close"].diff()) * df["volume"]).cumsum()
    df["obv"]             = obv
    df["obv_ema"]         = ema(obv, obv_len).round(2)
    df["obv_oscillator"]  = (obv - ema(obv, obv_len)).round(2)
    df["obv_above_zero"]  = df["obv_oscillator"] > 0
    df["obv_rising"]      = df["obv_oscillator"] > df["obv_oscillator"].shift(1)
    df["obv_trending_up"] = df["obv_above_zero"] & df["obv_rising"]
    return df

def compute_volume_trending_up(df, cfg):
    vs = cfg.get("volume_short_ma", 5)
    vl = cfg.get("volume_long_ma",  20)
    df["vol_ma_short"] = df["volume"].rolling(vs).mean()
    df["vol_ma_long"]  = df["volume"].rolling(vl).mean()
    df["vol_trend_up"] = df["vol_ma_short"] > df["vol_ma_long"]
    return df

# ── Fetch bars from Alpaca ────────────────────────────────────────────────────
print(f"\nFetching {bar_count} x {tf} bars for {ticker} ({'PAPER' if PAPER else 'LIVE'})…")

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests   import StockBarsRequest
from alpaca.data.timeframe  import TimeFrame, TimeFrameUnit

tf_map = {
    "1Min":  TimeFrame(1,  TimeFrameUnit.Minute),
    "5Min":  TimeFrame(5,  TimeFrameUnit.Minute),
    "15Min": TimeFrame(15, TimeFrameUnit.Minute),
    "1Hour": TimeFrame(1,  TimeFrameUnit.Hour),
    "1Day":  TimeFrame(1,  TimeFrameUnit.Day),
}
timeframe = tf_map.get(tf, TimeFrame(5, TimeFrameUnit.Minute))

data = StockHistoricalDataClient(API_KEY, SECRET_KEY)
req  = StockBarsRequest(
    symbol_or_symbols=ticker,
    timeframe=timeframe,
    start=datetime.now(timezone.utc) - timedelta(days=10),
    limit=bar_count,
)
raw = data.get_stock_bars(req).df
if raw.empty:
    print(f"ERROR: No bars returned for {ticker}")
    sys.exit(1)
if isinstance(raw.index, pd.MultiIndex):
    raw = raw.xs(ticker, level="symbol")
df = raw[["open","high","low","close","volume"]].copy()
df["close"] = pd.to_numeric(df["close"], errors="coerce")
df = df.dropna(subset=["close"])

print(f"Got {len(df)} bars  ({df.index[0]}  →  {df.index[-1]})\n")

# ── Run indicators ────────────────────────────────────────────────────────────
df = compute_percent_r_exhaustion(df, cfg)
df = compute_cm_rsi_lower(df, cfg)
df = compute_obv_oscillator(df, cfg)
df = compute_macd(df, cfg)
df = compute_volume_trending_up(df, cfg)

cm_thresh     = cfg.get("cm_rsi_approaching", 25)
min_bars      = cfg.get("rte_min_bars", 2)

# ── Print last N rows ─────────────────────────────────────────────────────────
tail = df.tail(show_rows)

# Column definitions: (key, header, width, format)
COLS = [
    ("close",               "close",    7,  lambda v: f"{v:7.3f}"),
    ("rte_final",            "rte",      7,  lambda v: f"{v:7.2f}"),
    ("ob_consecutive",       "cnt",      3,  lambda v: f"{int(v):>3}"),
    ("ob_on_deck",           "ondeck",   6,  lambda v: " ✓ ON" if v else " ✗   "),
    ("ob_reversal",          "offdeck",  7,  lambda v: " ✓ OFF" if v else " ✗    "),
    ("cm_rsi",               "cmRSI",    6,  lambda v: f"{v:6.2f}"),
    ("cm_rsi_approaching",   "cmAppr",   6,  lambda v: " ✓" if v else " ✗"),
    ("obv_oscillator",       "obv",      9,  lambda v: f"{v:9.2f}"),
    ("obv_trending_up",      "obvUp",    5,  lambda v: " ✓" if v else " ✗"),
    ("macd_bull",            "m↑",       2,  lambda v: " ✓" if v else " ✗"),
]

# Header row
hdr_time = f"{'time':<19}"
hdr_cols = "  ".join(f"{c[1]:>{c[2]}}" for c in COLS)
sep = "-" * (19 + 2 + len(hdr_cols))
print(sep)
print(f"{hdr_time}  {hdr_cols}")
print(sep)

for ts, row in tail.iterrows():
    t_str = str(ts)[:19]
    vals  = "  ".join(fmt(row[key]) for key, _, width, fmt in COLS)
    print(f"{t_str}  {vals}")

print(sep)

# ── Summary of last bar ───────────────────────────────────────────────────────
last = df.iloc[-1]
print("\n" + "="*70)
print(f"  LAST BAR SUMMARY  —  {ticker}  @  {df.index[-1]}")
print("="*70)

# %R Trend Exhaustion
rte_on_deck   = bool(last["ob_on_deck"])
rte_off_deck  = bool(last["ob_reversal"])
rte_count     = int(last["ob_consecutive"])
print(f"\n  %R TREND EXHAUSTION:")
print(f"    rte_final={last['rte_final']:.2f}  consecutive={rte_count}/{min_bars}")
print(f"    ON DECK: {rte_on_deck} (need {min_bars}+ bars in overbought)")
print(f"    OFF DECK: {rte_off_deck} (reversal from overbought)")

# CM RSI-2
rsi_approaching = bool(last["cm_rsi_approaching"])
print(f"\n  CM RSI-2:")
print(f"    cm_rsi={last['cm_rsi']:.2f}  approaching={rsi_approaching}")
print(f"    (need RSI<{cm_thresh} AND close>SMA200 AND close<SMA5)")

# OBV Oscillator
obv_ok = bool(last["obv_trending_up"])
print(f"\n  OBV OSCILLATOR:")
print(f"    obv_oscillator={last['obv_oscillator']:.2f}  trending_up={obv_ok}")
print(f"    (need > 0 AND rising)")

# MACD
macd_ok = bool(last["macd_bull"])
print(f"\n  MACD:")
print(f"    macd={last['macd_line']:.4f}  signal={last['macd_signal_line']:.4f}")
print(f"    bull_cross={macd_ok}")

# Overall signal
print(f"\n  {'='*70}")
all_pass = rte_off_deck and rsi_approaching and obv_ok and macd_ok
print(f"  → {'BUY SIGNAL would fire' if all_pass else 'No signal on last bar'}")
print(f"  {'='*70}\n")
