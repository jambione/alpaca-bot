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
        extreme = final >= -threshold
    else:
        extreme = final <= (-100 + threshold)
    reversal = (~extreme) & extreme.shift(1).fillna(False)
    boxes    = reversal.cumsum().fillna(0).astype(int)
    df["rte_final"]           = final.round(2)
    df["rte_extreme"]         = extreme
    df["rte_reversal"]        = reversal
    df["rte_boxes_completed"] = boxes
    return df

def compute_cm_rsi_lower(df, cfg):
    delta = df["close"].diff()
    up    = delta.clip(lower=0).ewm(alpha=0.5, adjust=False).mean()
    down  = (-delta.clip(upper=0)).ewm(alpha=0.5, adjust=False).mean()
    rsi2  = np.where(down == 0, 100, np.where(up == 0, 0, 100 - (100 / (1 + up / down))))
    df["cm_rsi"] = pd.Series(rsi2, index=df.index).round(2)
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
df = compute_macd(df, cfg)
df = compute_volume_trending_up(df, cfg)

cm_thresh = cfg.get("cm_rsi_threshold", 10)
min_boxes = cfg.get("rte_min_boxes", 3)

# ── Print last N rows ─────────────────────────────────────────────────────────
tail = df.tail(show_rows)

# Column definitions: (key, header, width, format)
COLS = [
    ("close",               "close",    7,  lambda v: f"{v:7.3f}"),
    ("rte_final",           "rte",      7,  lambda v: f"{v:7.2f}"),
    ("rte_extreme",         "ext",      3,  lambda v: " ✓ " if v else " ✗ "),
    ("rte_reversal",        "rev",      3,  lambda v: " ✓ " if v else " ✗ "),
    ("rte_boxes_completed", "box",      3,  lambda v: f"{int(v):>3}"),
    ("cm_rsi",              "cmRSI",    6,  lambda v: f"{v:6.2f}"),
    ("macd_line",           "macd",     8,  lambda v: f"{v:8.4f}"),
    ("macd_signal_line",    "sig",      8,  lambda v: f"{v:8.4f}"),
    ("macd_bull",           "m↑",       2,  lambda v: " ✓" if v else " ✗"),
    ("macd_bear",           "m↓",       2,  lambda v: " ✓" if v else " ✗"),
    ("vol_trend_up",        "vol↑",     4,  lambda v: "  ✓ " if v else "  ✗ "),
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
print("\n" + "="*60)
print(f"  LAST BAR SUMMARY  —  {ticker}  @  {df.index[-1]}")
print("="*60)
rte_ok  = bool(last["rte_reversal"]) and int(last["rte_boxes_completed"]) == min_boxes
rsi_ok  = float(last["cm_rsi"]) < cm_thresh
vol_ok  = bool(last["vol_trend_up"])
macd_ok = bool(last["macd_bull"])

def chk(v): return "✓ PASS" if v else "✗ FAIL"

print(f"  RTE reversal + boxes={int(last['rte_boxes_completed'])}/{min_boxes}  {chk(rte_ok)}")
print(f"    rte_final={last['rte_final']:.2f}  extreme={bool(last['rte_extreme'])}  reversal={bool(last['rte_reversal'])}")
print(f"  CM RSI = {last['cm_rsi']:.2f}  (need < {cm_thresh})  {chk(rsi_ok)}")
print(f"  Volume trending up                               {chk(vol_ok)}")
print(f"  MACD bull crossover                              {chk(macd_ok)}")
print(f"    macd={last['macd_line']:.4f}  signal={last['macd_signal_line']:.4f}  hist={last['macd_hist']:.4f}")
print()
all_pass = rte_ok and rsi_ok and vol_ok and macd_ok
print(f"  → {'BUY SIGNAL would fire' if all_pass else 'No signal on last bar'}")
print("="*60 + "\n")
