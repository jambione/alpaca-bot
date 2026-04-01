#!/usr/bin/env python3
"""
Quick pre-scan: evaluate a ticker list against the exhaustion strategy
and print On Deck / Buy Signal / Warming Up tiers.

Run from the alpaca_trading_bot directory:
  python3 scan_watchlist.py
"""
import sys, os, json, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(__file__))

import signals as _sig
from config import DEFAULT_CONFIG

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta, timezone

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "bot_config.json")
cfg = dict(DEFAULT_CONFIG)
try:
    with open(CONFIG_FILE) as f:
        raw = f.read().strip()
    # strip trailing commas that break json.loads
    import re
    raw = re.sub(r',\s*([}\]])', r'\1', raw)
    cfg.update(json.loads(raw))
except Exception as e:
    print(f"  [warn] could not load bot_config.json ({e}), using defaults")

API_KEY    = cfg.get("api_key",    os.getenv("ALPACA_API_KEY",    ""))
SECRET_KEY = cfg.get("secret_key", os.getenv("ALPACA_SECRET_KEY", ""))

if not API_KEY or not SECRET_KEY:
    print("ERROR: no API credentials. Set ALPACA_API_KEY / ALPACA_SECRET_KEY or check bot_config.json")
    sys.exit(1)

# ── Ticker list ───────────────────────────────────────────────────────────────
TICKERS = [t.strip().upper() for t in cfg.get("tickers", []) if t and t.strip()]
if not TICKERS:
    print("ERROR: no tickers defined in bot_config.json")
    sys.exit(1)

# ── Fetch bars ────────────────────────────────────────────────────────────────
data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
end   = datetime.now(timezone.utc)
start = end - timedelta(hours=20)

print(f"Fetching 1-min bars for {len(TICKERS)} tickers…")
req = StockBarsRequest(
    symbol_or_symbols=TICKERS,
    timeframe=TimeFrame.Minute,
    start=start,
    end=end,
    limit=1200,
    feed="iex",   # free-tier accounts use IEX, not SIP
)
bars = data_client.get_stock_bars(req)
bar_df = bars.df
got_syms = bar_df.index.get_level_values(0).unique().tolist()
print(f"  → Got data for {len(got_syms)} symbols\n")

# ── Evaluate signals ──────────────────────────────────────────────────────────
results = []
for ticker in TICKERS:
    try:
        if ticker not in got_syms:
            results.append({"t": ticker, "status": "NO_DATA"})
            continue
        df = bar_df.loc[ticker].copy().reset_index()
        df = df.rename(columns={"timestamp": "time"})
        if len(df) < 50:
            results.append({"t": ticker, "status": f"THIN ({len(df)} bars)"})
            continue
        df = _sig.compute_signals(df, cfg)
        lat = df.iloc[-1]

        def _f(col, default=0.0):
            v = lat.get(col, default)
            return float(v) if v is not None else default

        def _b(col):
            return bool(lat.get(col, False))

        price    = _f("close")
        rte_fast = _f("rte_fast", -999)
        rte_slow = _f("rte_slow", -999)
        streak   = int(_f("rte_boxes_streak"))
        reversal = _b("rte_reversal")
        buy_sig  = _b("buy")
        rsi      = _f("rsi", 50)
        above_vwap = _b("above_vwap")
        vol_up   = _b("vol_trend_up")
        vol_surge = _b("vol_surge")
        macd_ok  = _f("macd_line") > _f("macd_signal_line")
        mq       = _b("momentum_quality")

        # How many supporting conditions pass
        support  = sum([_b("rmi_signal") or mq, vol_up or vol_surge, macd_ok])
        min_sup  = cfg.get("rte_min_supporting", 1)

        results.append({
            "t": ticker, "status": "OK", "price": price,
            "rte_fast": rte_fast, "rte_slow": rte_slow,
            "streak": streak, "reversal": reversal,
            "buy": buy_sig, "support": support, "min_sup": min_sup,
            "rsi": rsi, "above_vwap": above_vwap,
            "vol_up": vol_up, "vol_surge": vol_surge,
            "macd_ok": macd_ok, "mq": mq,
        })
    except Exception as ex:
        results.append({"t": ticker, "status": f"ERR: {ex}"})

# ── Tier sort ─────────────────────────────────────────────────────────────────
ok      = [r for r in results if r.get("status") == "OK"]
BUY     = [r for r in ok if r["buy"]]
ONDECK  = [r for r in ok if r["streak"] >= 1 and not r["buy"]]
WARMING = [r for r in ok if r["streak"] == 0 and r["rte_fast"] > -60]
COLD    = [r for r in ok if r["streak"] == 0 and r["rte_fast"] <= -60]
SKIPPED = [r for r in results if r["status"] != "OK"]

def row(r):
    return (
        f"  {r['t']:<6} ${r['price']:>7.3f}"
        f"  fast={r['rte_fast']:>5.0f}  slow={r['rte_slow']:>5.0f}"
        f"  streak={r['streak']}"
        f"  RSI={r['rsi']:>4.0f}"
        f"  VWAP={'✓' if r['above_vwap'] else '✗'}"
        f"  vol={'✓' if r['vol_up'] or r['vol_surge'] else '✗'}"
        f"  macd={'✓' if r['macd_ok'] else '✗'}"
        f"  mq={'✓' if r['mq'] else '✗'}"
        f"  sup={r['support']}/{r['min_sup']}"
        + ("  ← REVERSAL BAR" if r["reversal"] else "")
    )

W = 80
print("=" * W)
print(f"  🟢  BUY SIGNAL  ({len(BUY)} ticker{'s' if len(BUY)!=1 else ''})")
print("=" * W)
if BUY:
    for r in sorted(BUY, key=lambda x: x["rte_fast"], reverse=True):
        print(row(r))
else:
    print("  (none right now)")

print()
print("=" * W)
print(f"  🟡  ON DECK — building toward signal  ({len(ONDECK)} ticker{'s' if len(ONDECK)!=1 else ''})")
print("=" * W)
if ONDECK:
    for r in sorted(ONDECK, key=lambda x: (x["streak"], x["rte_fast"]), reverse=True):
        print(row(r))
else:
    print("  (none)")

print()
print("=" * W)
print(f"  🔵  WARMING UP — fast %R > -60, no streak yet  ({len(WARMING)})")
print("=" * W)
if WARMING:
    for r in sorted(WARMING, key=lambda x: x["rte_fast"], reverse=True)[:15]:
        print(row(r))

print()
print("=" * W)
print(f"  ❄️   COLD — not in zone yet  ({len(COLD)})")
print("=" * W)
for r in sorted(COLD, key=lambda x: x["rte_fast"], reverse=True)[:10]:
    print(f"  {r['t']:<6} ${r['price']:>7.3f}  fast={r['rte_fast']:>5.0f}  slow={r['rte_slow']:>5.0f}  RSI={r['rsi']:.0f}")

if SKIPPED:
    print()
    print("SKIPPED / NO DATA:")
    for r in SKIPPED:
        print(f"  {r['t']}: {r['status']}")
