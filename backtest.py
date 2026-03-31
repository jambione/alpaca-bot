#!/usr/bin/env python3
"""
Alpaca Trading Bot — Backtester
================================
Reuses the live bot's exact signal engine (compute_signals) on historical
1-minute bars and simulates trade entries/exits with the same stop/target/
trail/partial logic the live bot uses.

Usage examples
--------------
  # Last 30 days on two tickers
  python backtest.py --tickers GVH SOXS --days 30

  # Specific date range
  python backtest.py --tickers GVH --start 2025-01-01 --end 2025-03-01

  # Override risk params for a test run
  python backtest.py --tickers GVH --days 60 --stop 3 --target 10

  # Multiple tickers, custom output folder
  python backtest.py --tickers GVH NVDA SMCI --days 45 --out ./results
"""

import sys, os, json, csv, argparse, textwrap
from datetime import datetime, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np

# ── Import signal engine from dashboard (pure functions only) ─────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

try:
    # Stub out web-server modules so alpaca_dashboard.py can be imported
    # even when uvicorn/fastapi are not installed in this environment.
    import types as _types, sys as _sys

    def _noop_decorator(*a, **kw):
        """Return a pass-through decorator."""
        def _dec(f): return f
        return _dec

    class _FakeApp:
        """Minimal FastAPI/Starlette app shim — just enough for module import."""
        def __init__(self, **kw): pass
        def get(self, *a, **kw):      return _noop_decorator(*a, **kw)
        def post(self, *a, **kw):     return _noop_decorator(*a, **kw)
        def delete(self, *a, **kw):   return _noop_decorator(*a, **kw)
        def put(self, *a, **kw):      return _noop_decorator(*a, **kw)
        def on_event(self, *a, **kw): return _noop_decorator(*a, **kw)
        def mount(self, *a, **kw):    pass
        def add_middleware(self, *a, **kw): pass

    class _FakeResponse:
        def __init__(self, *a, **kw): pass

    # Build stub modules
    for _mod_name, _attrs in [
        ("uvicorn", {"run": lambda *a, **kw: None}),
        ("fastapi",         {"FastAPI": _FakeApp, "Request": object, "HTTPException": Exception}),
        ("fastapi.responses",{"HTMLResponse": _FakeResponse, "StreamingResponse": _FakeResponse, "JSONResponse": _FakeResponse}),
        ("fastapi.staticfiles", {"StaticFiles": object}),
        ("fastapi.middleware", {}),
        ("fastapi.middleware.cors", {"CORSMiddleware": object}),
        ("starlette", {}),
        ("starlette.responses", {"Response": _FakeResponse}),
        ("starlette.requests",  {"Request": object}),
    ]:
        if _mod_name not in _sys.modules:
            _m = _types.ModuleType(_mod_name)
            for _k, _v in _attrs.items():
                setattr(_m, _k, _v)
            _sys.modules[_mod_name] = _m
        else:
            # Patch missing attrs into already-registered stubs
            _m = _sys.modules[_mod_name]
            for _k, _v in _attrs.items():
                if not hasattr(_m, _k):
                    setattr(_m, _k, _v)

    from alpaca_dashboard import compute_signals, DEFAULT_CONFIG, load_config
except ImportError as e:
    print(f"ERROR: Could not import from alpaca_dashboard.py: {e}")
    sys.exit(1)

# ── Alpaca data client ────────────────────────────────────────────────────
try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests  import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
except ImportError:
    print("ERROR: alpaca-py not installed.  Run: pip install alpaca-py --break-system-packages")
    sys.exit(1)

ET = ZoneInfo("America/New_York")

# ═══════════════════════════════════════════════════════════
#  BAR FETCHING
# ═══════════════════════════════════════════════════════════

def fetch_bars_range(client, ticker: str, start: datetime, end: datetime,
                     timeframe: str = "1Min") -> pd.DataFrame | None:
    """
    Fetch all 1Min bars between start and end (timezone-aware datetimes).
    Handles pagination automatically via alpaca-py.
    Adds a warm-up buffer of 5 trading days before `start` so indicators
    (SMA-200 etc.) have enough history to be accurate at the start date.
    """
    tf_map = {
        "1Min":  TimeFrame(1,  TimeFrameUnit.Minute),
        "5Min":  TimeFrame(5,  TimeFrameUnit.Minute),
        "15Min": TimeFrame(15, TimeFrameUnit.Minute),
        "1Hour": TimeFrame(1,  TimeFrameUnit.Hour),
    }
    tf = tf_map.get(timeframe, TimeFrame(1, TimeFrameUnit.Minute))

    # Pull extra history for indicator warm-up (SMA-200 needs 200 bars + buffer)
    warmup_start = start - timedelta(days=7)  # ~7 calendar days ≈ 5 trading days

    # Detect feed from alpaca-py — IEX is available on all plans including paper/free.
    # SIP (consolidated tape) requires a paid data subscription.
    try:
        from alpaca.data.enums import DataFeed
        _feed = DataFeed.IEX
    except Exception:
        _feed = None

    try:
        req_kwargs = dict(
            symbol_or_symbols=ticker,
            timeframe=tf,
            start=warmup_start,
            end=end,
        )
        if _feed is not None:
            req_kwargs["feed"] = _feed
        req = StockBarsRequest(**req_kwargs)
        bars = client.get_stock_bars(req).df
        if bars is None or bars.empty:
            return None
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(ticker, level="symbol")
        bars = bars[["open","high","low","close","volume"]].copy()
        bars["close"] = pd.to_numeric(bars["close"], errors="coerce")
        bars = bars.dropna(subset=["close"])
        # Convert index to ET so intraday time filters work correctly
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC")
        bars.index = bars.index.tz_convert(ET)
        return bars
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None


# ═══════════════════════════════════════════════════════════
#  SIMULATION ENGINE
# ═══════════════════════════════════════════════════════════

def simulate(df: pd.DataFrame, cfg: dict, ticker: str,
             warmup_cutoff: datetime,
             no_signal_sell: bool = False,
             regime_dates: set = None) -> list[dict]:
    """
    Walk bar-by-bar through a signals DataFrame and simulate trades.
    - Entry  : BUY signal fires → enter at NEXT bar's open (realistic fill)
    - Exit   : STOP / TARGET / TRAIL / EOD (+ EMA-cross SELL unless no_signal_sell=True)
    - warmup_cutoff: ignore signals before this timestamp (indicator warm-up)
    - no_signal_sell: if True, ignore EMA-cross SELL exits — let stop/target/trail work

    Returns a list of trade dicts.
    """
    stop_pct         = cfg.get("stop_loss_pct",        0.04)
    target_pct       = cfg.get("take_profit_pct",       0.12)
    trail_act_pct    = cfg.get("trail_activation_pct",  0.05)
    trail_pct        = cfg.get("trail_stop_pct",        0.02)
    partial_pct      = cfg.get("partial_exit_pct",      0.06)
    partial_qty_pct  = cfg.get("partial_exit_qty_pct",  0.50)
    no_buy_h, no_buy_m = cfg.get("no_new_buys_after",  [15, 30])
    eod_h,    eod_m    = cfg.get("eod_liquidate_at",    [16,  0])

    trades   = []
    position = None  # open trade dict, or None

    bars = list(df.iterrows())  # list of (timestamp, row) tuples

    for idx, (ts, row) in enumerate(bars):
        price  = float(row["close"])
        signal = str(row.get("signal", "HOLD"))

        # ── Manage open position ─────────────────────────────────────────
        if position is not None:
            entry      = position["entry_price"]
            high_water = position["high_water"]
            pct_gain   = (price - entry) / entry

            # Update high-water mark
            if price > high_water:
                position["high_water"] = price
                high_water = price

            exit_reason = None
            exit_price  = price

            # EOD liquidation
            if ts.hour > eod_h or (ts.hour == eod_h and ts.minute >= eod_m):
                exit_reason = "EOD"

            # Hard stop
            elif price <= entry * (1 - stop_pct):
                exit_reason = "STOP"
                exit_price  = entry * (1 - stop_pct)  # worst-case fill

            # Take profit
            elif price >= entry * (1 + target_pct):
                exit_reason = "TARGET"
                exit_price  = entry * (1 + target_pct)

            # Trailing stop (activates after trail_activation_pct gain)
            elif pct_gain >= trail_act_pct:
                trail_stop = high_water * (1 - trail_pct)
                if price <= trail_stop:
                    exit_reason = "TRAIL"
                    exit_price  = trail_stop

            # EMA-cross / bearish signal exit (skipped if --no-signal-sell)
            elif signal == "SELL" and not no_signal_sell:
                exit_reason = "SIGNAL_SELL"

            # Partial exit (recorded but position stays open)
            if not position["partial_done"] and pct_gain >= partial_pct:
                position["partial_done"]  = True
                position["partial_price"] = price
                position["partial_gain"]  = round(pct_gain * 100, 2)

            # Close position
            if exit_reason:
                raw_pnl = (exit_price - entry) / entry
                # If partial exit occurred, blend the return
                if position["partial_done"]:
                    partial_return = (position["partial_price"] - entry) / entry
                    remaining_return = (exit_price - entry) / entry
                    blended = (partial_qty_pct * partial_return
                               + (1 - partial_qty_pct) * remaining_return)
                    final_pnl = blended
                else:
                    final_pnl = raw_pnl

                hold_mins = int((ts - position["entry_ts"]).total_seconds() / 60)

                trades.append({
                    "ticker":         ticker,
                    "date":           position["entry_ts"].strftime("%Y-%m-%d"),
                    "entry_ts":       position["entry_ts"].strftime("%H:%M"),
                    "exit_ts":        ts.strftime("%H:%M"),
                    "entry_price":    round(entry, 4),
                    "exit_price":     round(exit_price, 4),
                    "pnl_pct":        round(final_pnl * 100, 2),
                    "pnl_r":          round(final_pnl / stop_pct, 2),  # R-multiple
                    "hold_mins":      hold_mins,
                    "exit_reason":    exit_reason,
                    "streak":         position["streak"],
                    "float_m":        position["float_m"],
                    "rvol":           position["rvol"],
                    "rsi2":           position["rsi2"],
                    "rte_fast":       position["rte_fast"],
                    "rte_slow":       position["rte_slow"],
                    "partial_done":   position["partial_done"],
                    "partial_gain":   position.get("partial_gain", ""),
                    "win":            final_pnl > 0,
                })
                position = None

        # ── Entry ────────────────────────────────────────────────────────
        if position is None and signal == "BUY":
            # Skip warm-up period
            if ts < warmup_cutoff:
                continue
            # Regime filter: skip entry if this day is not in allowed dates
            if regime_dates is not None and ts.date() not in regime_dates:
                continue
            # No new buys after cutoff time
            if ts.hour > no_buy_h or (ts.hour == no_buy_h and ts.minute >= no_buy_m):
                continue
            # Enter at next bar's open for realism (skip if last bar)
            if idx + 1 >= len(bars):
                continue
            next_open = float(bars[idx + 1][1]["open"])

            position = {
                "entry_ts":     bars[idx + 1][0],  # timestamp of next bar
                "entry_price":  next_open,
                "high_water":   next_open,
                "partial_done": False,
                "streak":       int(row.get("rte_boxes_streak", 0)),
                "float_m":      0.0,    # set after call (passed in separately)
                "rvol":         float(row.get("rvol", 0)) if "rvol" in df.columns else 0.0,
                "rsi2":         round(float(row.get("rmi", 50)), 1),
                "rte_fast":     round(float(row.get("rte_fast", -100)), 1),
                "rte_slow":     round(float(row.get("rte_slow", -100)), 1),
            }

    # Force-close any position still open at end of data
    if position is not None:
        last_price = float(df.iloc[-1]["close"])
        last_ts    = df.index[-1]
        pnl        = (last_price - position["entry_price"]) / position["entry_price"]
        hold_mins  = int((last_ts - position["entry_ts"]).total_seconds() / 60)
        trades.append({
            "ticker":      ticker,
            "date":        position["entry_ts"].strftime("%Y-%m-%d"),
            "entry_ts":    position["entry_ts"].strftime("%H:%M"),
            "exit_ts":     last_ts.strftime("%H:%M"),
            "entry_price": round(position["entry_price"], 4),
            "exit_price":  round(last_price, 4),
            "pnl_pct":     round(pnl * 100, 2),
            "pnl_r":       round(pnl / stop_pct, 2),
            "hold_mins":   hold_mins,
            "exit_reason": "END_OF_DATA",
            "streak":      position["streak"],
            "float_m":     position["float_m"],
            "rvol":        position["rvol"],
            "rsi2":        position["rsi2"],
            "rte_fast":    position["rte_fast"],
            "rte_slow":    position["rte_slow"],
            "partial_done": position["partial_done"],
            "partial_gain": position.get("partial_gain", ""),
            "win":         pnl > 0,
        })

    return trades


# ═══════════════════════════════════════════════════════════
#  STATISTICS
# ═══════════════════════════════════════════════════════════

def compute_stats(trades: list[dict]) -> dict:
    if not trades:
        return {}

    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    pnls   = [t["pnl_pct"] for t in trades]
    rs     = [t["pnl_r"]   for t in trades]

    win_rate   = len(wins) / len(trades) * 100
    avg_win    = np.mean([t["pnl_pct"] for t in wins])   if wins   else 0
    avg_loss   = np.mean([t["pnl_pct"] for t in losses]) if losses else 0
    avg_r      = np.mean(rs)
    expectancy = (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)

    # Equity curve (1 = flat, compound)
    equity = 1.0
    curve  = []
    for t in trades:
        equity *= (1 + t["pnl_pct"] / 100)
        curve.append(round(equity, 4))

    # Max drawdown on equity curve
    peak = 1.0
    max_dd = 0.0
    for e in curve:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd

    # Breakdown by exit reason
    by_reason = {}
    for t in trades:
        r = t["exit_reason"]
        by_reason.setdefault(r, {"count": 0, "wins": 0, "total_pnl": 0.0})
        by_reason[r]["count"]     += 1
        by_reason[r]["wins"]      += int(t["win"])
        by_reason[r]["total_pnl"] += t["pnl_pct"]

    # Breakdown by streak
    by_streak = {}
    for t in trades:
        s = t["streak"]
        by_streak.setdefault(s, {"count": 0, "wins": 0, "total_pnl": 0.0})
        by_streak[s]["count"]     += 1
        by_streak[s]["wins"]      += int(t["win"])
        by_streak[s]["total_pnl"] += t["pnl_pct"]

    return {
        "total_trades": len(trades),
        "wins":         len(wins),
        "losses":       len(losses),
        "win_rate":     round(win_rate, 1),
        "avg_win_pct":  round(avg_win,    2),
        "avg_loss_pct": round(avg_loss,   2),
        "avg_r":        round(avg_r,      2),
        "expectancy":   round(expectancy, 2),
        "total_pnl":    round(sum(pnls),  2),
        "max_drawdown": round(max_dd * 100, 2),
        "equity_curve": curve,
        "by_reason":    by_reason,
        "by_streak":    by_streak,
    }


# ═══════════════════════════════════════════════════════════
#  HTML REPORT
# ═══════════════════════════════════════════════════════════

def build_html_report(all_trades: list[dict], stats_per_ticker: dict,
                      combined_stats: dict, cfg: dict,
                      start_date: str, end_date: str) -> str:

    tickers_json = json.dumps(list(stats_per_ticker.keys()))
    combined_json = json.dumps(combined_stats)

    # Build per-ticker stats rows
    ticker_rows = ""
    for t, s in stats_per_ticker.items():
        if not s:
            continue
        color = "pos" if s.get("total_pnl", 0) >= 0 else "neg"
        ticker_rows += f"""
        <tr>
          <td><strong>{t}</strong></td>
          <td>{s.get('total_trades',0)}</td>
          <td>{s.get('win_rate',0)}%</td>
          <td>{s.get('avg_win_pct',0):+.2f}%</td>
          <td>{s.get('avg_loss_pct',0):+.2f}%</td>
          <td>{s.get('avg_r',0):+.2f}R</td>
          <td>{s.get('expectancy',0):+.2f}%</td>
          <td class="{color}">{s.get('total_pnl',0):+.2f}%</td>
          <td class="neg">{s.get('max_drawdown',0):.1f}%</td>
        </tr>"""

    # Build trade rows
    trade_rows = ""
    for t in all_trades:
        cls = "win-row" if t["win"] else "loss-row"
        trade_rows += f"""
        <tr class="{cls}">
          <td>{t['ticker']}</td>
          <td>{t['date']}</td>
          <td>{t['entry_ts']}</td>
          <td>{t['exit_ts']}</td>
          <td>${t['entry_price']}</td>
          <td>${t['exit_price']}</td>
          <td class="{'pos' if t['win'] else 'neg'}">{t['pnl_pct']:+.2f}%</td>
          <td class="{'pos' if t['pnl_r']>0 else 'neg'}">{t['pnl_r']:+.2f}R</td>
          <td>{t['hold_mins']}m</td>
          <td>{t['exit_reason']}</td>
          <td>{t['streak']}</td>
          <td>{t['float_m'] if t['float_m'] else '?'}M</td>
          <td>{t['rvol']:.1f}x</td>
          <td>{t['rsi2']:.1f}</td>
        </tr>"""

    # Equity curve data
    eq_labels = json.dumps([t["date"] + " " + t["exit_ts"] for t in all_trades])
    eq_data   = json.dumps(combined_stats.get("equity_curve", []))

    # Summary cards
    cs = combined_stats
    wr_color = "#00c853" if cs.get("win_rate",0) >= 50 else "#ff5252"
    exp_color = "#00c853" if cs.get("expectancy",0) >= 0 else "#ff5252"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Backtest Report — {start_date} to {end_date}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0d1117;color:#c9d1d9;font-family:'Segoe UI',system-ui,sans-serif;font-size:13px;padding:24px}}
  h1{{font-size:20px;color:#e6edf3;margin-bottom:4px}}
  .subtitle{{color:#8b949e;margin-bottom:24px;font-size:12px}}
  .cards{{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:24px}}
  .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px 20px;min-width:130px}}
  .card-label{{font-size:11px;color:#8b949e;margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}}
  .card-value{{font-size:22px;font-weight:700;color:#e6edf3}}
  .chart-wrap{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin-bottom:24px}}
  .chart-wrap h2{{font-size:14px;color:#8b949e;margin-bottom:12px}}
  canvas{{max-height:260px}}
  table{{width:100%;border-collapse:collapse;background:#161b22;border:1px solid #30363d;border-radius:8px;overflow:hidden;margin-bottom:24px}}
  th{{background:#21262d;color:#8b949e;font-weight:600;font-size:11px;text-transform:uppercase;padding:8px 10px;text-align:left;border-bottom:1px solid #30363d}}
  td{{padding:6px 10px;border-bottom:1px solid #21262d;color:#c9d1d9}}
  tr:last-child td{{border-bottom:none}}
  .win-row td:first-child{{border-left:2px solid #00c853}}
  .loss-row td:first-child{{border-left:2px solid #ff5252}}
  .pos{{color:#00c853}}
  .neg{{color:#ff5252}}
  h2.section{{font-size:15px;color:#e6edf3;margin:0 0 12px 0}}
  .cfg-note{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px 16px;margin-bottom:24px;color:#8b949e;font-size:11px;line-height:1.8}}
  .cfg-note span{{color:#c9d1d9}}
</style>
</head>
<body>
<h1>Backtest Report</h1>
<div class="subtitle">{start_date} → {end_date} &nbsp;|&nbsp; Tickers: {', '.join(stats_per_ticker.keys())} &nbsp;|&nbsp; Timeframe: 1Min</div>

<div class="cards">
  <div class="card">
    <div class="card-label">Total Trades</div>
    <div class="card-value">{cs.get('total_trades',0)}</div>
  </div>
  <div class="card">
    <div class="card-label">Win Rate</div>
    <div class="card-value" style="color:{wr_color}">{cs.get('win_rate',0)}%</div>
  </div>
  <div class="card">
    <div class="card-label">Avg Winner</div>
    <div class="card-value" style="color:#00c853">{cs.get('avg_win_pct',0):+.1f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Avg Loser</div>
    <div class="card-value" style="color:#ff5252">{cs.get('avg_loss_pct',0):+.1f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Avg R</div>
    <div class="card-value" style="color:{'#00c853' if cs.get('avg_r',0)>0 else '#ff5252'}">{cs.get('avg_r',0):+.2f}R</div>
  </div>
  <div class="card">
    <div class="card-label">Expectancy / trade</div>
    <div class="card-value" style="color:{exp_color}">{cs.get('expectancy',0):+.2f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Total P&amp;L (compound)</div>
    <div class="card-value" style="color:{'#00c853' if cs.get('total_pnl',0)>=0 else '#ff5252'}">{cs.get('total_pnl',0):+.1f}%</div>
  </div>
  <div class="card">
    <div class="card-label">Max Drawdown</div>
    <div class="card-value" style="color:#ff5252">-{cs.get('max_drawdown',0):.1f}%</div>
  </div>
</div>

<div class="cfg-note">
  <strong>Strategy params:</strong> &nbsp;
  Stop <span>{cfg.get('stop_loss_pct',0.04)*100:.0f}%</span> &nbsp;|&nbsp;
  Target <span>{cfg.get('take_profit_pct',0.12)*100:.0f}%</span> &nbsp;|&nbsp;
  Trail activates @ <span>{cfg.get('trail_activation_pct',0.05)*100:.0f}%</span>, trails <span>{cfg.get('trail_stop_pct',0.02)*100:.0f}%</span> &nbsp;|&nbsp;
  Partial @ <span>{cfg.get('partial_exit_pct',0.06)*100:.0f}%</span> ({cfg.get('partial_exit_qty_pct',0.5)*100:.0f}% of position) &nbsp;|&nbsp;
  Min streak <span>{cfg.get('rte_min_boxes',2)}</span> boxes &nbsp;|&nbsp;
  Min supporting <span>{cfg.get('rte_min_supporting',1)}</span>
</div>

<div class="chart-wrap">
  <h2>Equity Curve (compounded per-trade, all tickers combined)</h2>
  <canvas id="equity-chart"></canvas>
</div>

<h2 class="section">Per-Ticker Summary</h2>
<table>
  <thead><tr>
    <th>Ticker</th><th>Trades</th><th>Win %</th>
    <th>Avg Win</th><th>Avg Loss</th><th>Avg R</th>
    <th>Expectancy</th><th>Total P&L</th><th>Max DD</th>
  </tr></thead>
  <tbody>{ticker_rows}</tbody>
</table>

<h2 class="section">All Trades</h2>
<table>
  <thead><tr>
    <th>Ticker</th><th>Date</th><th>Entry</th><th>Exit</th>
    <th>Entry $</th><th>Exit $</th>
    <th>P&L %</th><th>R</th><th>Hold</th><th>Exit Reason</th>
    <th>Streak</th><th>Float</th><th>RVOL</th><th>RSI-2</th>
  </tr></thead>
  <tbody>{trade_rows}</tbody>
</table>

<script>
const labels = {eq_labels};
const data   = {eq_data};
const ctx = document.getElementById('equity-chart').getContext('2d');
new Chart(ctx, {{
  type: 'line',
  data: {{
    labels,
    datasets: [{{
      label: 'Equity (1.0 = flat)',
      data,
      borderColor: data.length && data[data.length-1] >= 1.0 ? '#00c853' : '#ff5252',
      borderWidth: 2,
      pointRadius: 0,
      fill: true,
      backgroundColor: data.length && data[data.length-1] >= 1.0
        ? 'rgba(0,200,83,0.08)' : 'rgba(255,82,82,0.08)',
      tension: 0.2
    }}]
  }},
  options: {{
    animation: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ display: false }},
      y: {{
        grid: {{ color: 'rgba(48,54,61,0.6)' }},
        ticks: {{ color: '#8b949e' }},
      }}
    }}
  }}
}});
</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Backtest the exhaustion strategy on historical 1Min bars.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Examples:
          python backtest.py --tickers GVH --days 30
          python backtest.py --tickers GVH SOXS --start 2025-01-01 --end 2025-03-01
          python backtest.py --tickers GVH --days 60 --stop 3 --target 10 --min-streak 1
        """)
    )
    parser.add_argument("--tickers",    nargs="+", required=True,
                        help="One or more ticker symbols")
    parser.add_argument("--days",       type=int, default=30,
                        help="Number of calendar days to backtest (default 30)")
    parser.add_argument("--start",      type=str, default=None,
                        help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--end",        type=str, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--stop",       type=float, default=None,
                        help="Stop loss %% (e.g. 4 = 4%%)")
    parser.add_argument("--target",     type=float, default=None,
                        help="Take profit %% (e.g. 12 = 12%%)")
    parser.add_argument("--min-streak", type=int,   default=None,
                        help="Minimum box streak for entry (default from config)")
    parser.add_argument("--out",        type=str,   default="./backtest_results",
                        help="Output directory for CSV + HTML report")
    parser.add_argument("--no-html",         action="store_true",
                        help="Skip HTML report generation")
    parser.add_argument("--debug",           action="store_true",
                        help="Print signal chain diagnostics (why 0 trades)")
    parser.add_argument("--no-signal-sell",  action="store_true",
                        help="Ignore EMA-cross exits — let stop/target/trail manage the trade")
    parser.add_argument("--regime-filter",   action="store_true",
                        help="Only enter SQQQ when QQQ is in downtrend (below 20-day EMA)")
    parser.add_argument("--rte-side",        type=str, default=None,
                        choices=["red", "blue"],
                        help="Override %R exhaustion side: 'red'=overbought (near 0), 'blue'=oversold (near -100)")
    parser.add_argument("--set", nargs="+", default=[], metavar="KEY=VALUE",
                        help="Override any config key, e.g. --set rmi_ma_slow=50 use_rmi=false")
    args = parser.parse_args()

    # ── Load config and apply any CLI overrides ───────────────────────────
    cfg = {**DEFAULT_CONFIG}
    try:
        saved = load_config()
        cfg.update(saved)
    except Exception:
        pass

    if args.stop       is not None: cfg["stop_loss_pct"]    = args.stop / 100
    if args.target     is not None: cfg["take_profit_pct"]  = args.target / 100
    if args.min_streak is not None: cfg["rte_min_boxes"]     = args.min_streak
    if args.rte_side   is not None: cfg["rte_side"]          = args.rte_side

    # Apply --set key=value overrides with automatic type coercion
    for kv in args.set:
        if "=" not in kv:
            print(f"[WARN] --set '{kv}' ignored (no '=' found)")
            continue
        k, v = kv.split("=", 1)
        # Coerce type: bool → float → int → str
        if v.lower() in ("true", "false"):
            cfg[k] = v.lower() == "true"
        else:
            try:    cfg[k] = float(v) if "." in v else int(v)
            except: cfg[k] = v
        print(f"[SET] {k} = {cfg[k]!r}")

    # ── Date range ─────────────────────────────────────────────────────────
    now_et = datetime.now(ZoneInfo("America/New_York"))
    if args.end:
        end_dt = datetime.fromisoformat(args.end).replace(
            hour=23, minute=59, tzinfo=ZoneInfo("America/New_York"))
    else:
        end_dt = now_et

    if args.start:
        start_dt = datetime.fromisoformat(args.start).replace(
            hour=0, minute=0, tzinfo=ZoneInfo("America/New_York"))
    else:
        start_dt = end_dt - timedelta(days=args.days)

    start_str = start_dt.strftime("%Y-%m-%d")
    end_str   = end_dt.strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  Backtest: {start_str} → {end_str}")
    print(f"  Tickers:  {', '.join(args.tickers)}")
    print(f"  Stop: {cfg['stop_loss_pct']*100:.0f}%  "
          f"Target: {cfg['take_profit_pct']*100:.0f}%  "
          f"Min streak: {cfg.get('rte_min_boxes',2)}")
    print(f"{'='*60}\n")

    # ── Alpaca client ─────────────────────────────────────────────────────
    api_key = cfg.get("api_key", "")
    secret  = cfg.get("secret_key", "")
    if not api_key or not secret:
        print("ERROR: API key / secret not found in config.")
        sys.exit(1)
    client = StockHistoricalDataClient(api_key, secret)

    # ── Output directory ──────────────────────────────────────────────────
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Regime filter: pre-fetch QQQ daily data if needed ────────────────
    regime_allowed_dates = None   # None = no filter; set = allowed entry dates
    if args.regime_filter and any(t.upper() in cfg.get("regime_filters", {}) for t in args.tickers):
        print("  [REGIME] Fetching QQQ daily bars for trend filter …", end=" ", flush=True)
        try:
            from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
            from alpaca.data.enums import DataFeed
            tf_daily = TimeFrame(1, TimeFrameUnit.Day)
            from alpaca.data.requests import StockBarsRequest
            qqq_start = start_dt - timedelta(days=40)   # 40 days back for EMA20 warm-up
            req = StockBarsRequest(symbol_or_symbols="QQQ", timeframe=tf_daily,
                                   start=qqq_start, end=end_dt, feed=DataFeed.IEX)
            qqq_bars = client.get_stock_bars(req).df
            if isinstance(qqq_bars.index, pd.MultiIndex):
                qqq_bars = qqq_bars.xs("QQQ", level="symbol")
            if qqq_bars.index.tz is None:
                qqq_bars.index = qqq_bars.index.tz_localize("UTC")
            qqq_bars.index = qqq_bars.index.tz_convert(ET)
            qqq_bars["ema20"] = qqq_bars["close"].ewm(span=20, adjust=False).mean()
            # Days where QQQ closed BELOW its 20-day EMA = downtrend = SQQQ allowed
            downtrend_dates = set(
                qqq_bars[qqq_bars["close"] < qqq_bars["ema20"]].index.normalize().date
            )
            regime_allowed_dates = downtrend_dates
            print(f"OK — {len(downtrend_dates)} downtrend days found in window")
        except Exception as e:
            print(f"WARNING: regime filter fetch failed ({e}) — proceeding without filter")

    # ── Process each ticker ───────────────────────────────────────────────
    all_trades        = []
    stats_per_ticker  = {}

    for ticker in args.tickers:
        ticker = ticker.upper()
        print(f"  [{ticker}] Fetching bars …", end=" ", flush=True)

        bars = fetch_bars_range(client, ticker, start_dt, end_dt,
                                 cfg.get("bar_timeframe", "1Min"))
        if bars is None or bars.empty:
            print("NO DATA — skipped")
            stats_per_ticker[ticker] = {}
            continue

        print(f"{len(bars):,} bars received", end=" → ", flush=True)

        # Warm-up cutoff: ignore signals before our actual start date
        warmup_cutoff = start_dt

        # Per-ticker config overrides (e.g. different rte_min_boxes per ticker)
        ticker_cfg = dict(cfg)
        overrides = cfg.get("ticker_overrides", {}).get(ticker, {})
        if overrides:
            ticker_cfg.update(overrides)
            override_str = ", ".join(f"{k}={v}" for k, v in overrides.items())
            print(f"[override: {override_str}] ", end="", flush=True)

        # Run signal engine
        try:
            bars = compute_signals(bars, ticker_cfg)
        except Exception as e:
            print(f"signal error: {e} — skipped")
            stats_per_ticker[ticker] = {}
            continue

        # Count buy signals
        n_signals = int((bars["signal"] == "BUY").sum())
        print(f"{n_signals} BUY signals", end=" → ", flush=True)

        # ── Signal chain diagnostics ─────────────────────────────────────
        if args.debug or n_signals == 0:
            total = len(bars)
            after_warmup = bars[bars.index >= warmup_cutoff]
            aw = len(after_warmup)
            print()
            print(f"\n  ── Diagnostics: {ticker} ({aw:,} bars after warmup) ──")

            def _pct(col):
                if col not in bars.columns: return "N/A (column missing)"
                n = int(bars[col].astype(bool).sum())
                return f"{n:,} bars  ({n/total*100:.1f}%)"

            def _rng(col):
                if col not in bars.columns: return "N/A"
                s = bars[col].dropna()
                if s.empty: return "all NaN"
                return f"min={s.min():.2f}  max={s.max():.2f}  mean={s.mean():.2f}"

            # W%R exhaustion chain
            print(f"  W%%R fast (rte_fast)  range : {_rng('rte_fast')}")
            print(f"  W%%R slow (rte_slow)  range : {_rng('rte_slow')}")
            thr = -cfg.get("rte_threshold", 20)
            if "rte_fast" in bars.columns and "rte_slow" in bars.columns:
                n_extreme = int(((bars["rte_fast"] >= thr) & (bars["rte_slow"] >= thr)).sum())
                print(f"  Both lines in extreme zone (>={thr}) : {n_extreme:,} bars  ({n_extreme/total*100:.1f}%)")
            print(f"  rte_extreme           : {_pct('rte_extreme')}")
            print(f"  rte_reversal          : {_pct('rte_reversal')}")
            if "rte_boxes_streak" in bars.columns:
                streak_max = int(bars["rte_boxes_streak"].max())
                streak_gt0 = int((bars["rte_boxes_streak"] > 0).sum())
                print(f"  rte_boxes_streak > 0  : {streak_gt0:,} bars  (max streak={streak_max})")
            min_boxes = cfg.get("rte_min_boxes", 2)
            print(f"  rte_boxes_streak >= {min_boxes} : {_pct('rte_boxes_streak') if 'rte_boxes_streak' not in bars.columns else str(int((bars['rte_boxes_streak'] >= min_boxes).sum())) + ' bars'}")

            # RSI-2 chain
            print(f"  RSI-2 (rmi)           range : {_rng('rmi')}")
            rmi_thr = cfg.get("rmi_oversold", 10)
            if "rmi" in bars.columns:
                n_rmi_low = int((bars["rmi"] < rmi_thr).sum())
                print(f"  RSI-2 < {rmi_thr}             : {n_rmi_low:,} bars  ({n_rmi_low/total*100:.1f}%)")
            if "sma_slow" in bars.columns:
                n_above_sma = int((bars["close"] > bars["sma_slow"]).sum())
                print(f"  close > SMA(200)      : {n_above_sma:,} bars  ({n_above_sma/total*100:.1f}%)")
            if "sma_fast" in bars.columns:
                n_below_sma = int((bars["close"] < bars["sma_fast"]).sum())
                print(f"  close < SMA({cfg.get('rmi_ma_fast',20)})       : {n_below_sma:,} bars  ({n_below_sma/total*100:.1f}%)")
            print(f"  rmi_signal (all 3)    : {_pct('rmi_signal')}")

            # Supporting conditions
            print(f"  vol_trend_up          : {_pct('vol_trend_up')}")
            print(f"  macd_bull             : {_pct('macd_bull')}")

            # Final gate
            n_buy = int((bars["signal"] == "BUY").sum()) if "signal" in bars.columns else 0
            print(f"  signal == BUY         : {n_buy:,} bars  ({n_buy/total*100:.1f}%)")

            # Hint
            if "rte_extreme" in bars.columns and bars["rte_extreme"].sum() == 0:
                print(f"\n  ⚠  W%%R never reached extreme zone — price never sustained overbought")
                print(f"     Try: --rte-threshold 30 to widen the zone, or test a different ticker")
            elif "rte_reversal" in bars.columns and bars["rte_reversal"].sum() == 0:
                print(f"\n  ⚠  W%%R entered extreme zone but no reversals detected")
                print(f"     Entry window may be too narrow (rte_entry_window={cfg.get('rte_entry_window',10)})")
            elif n_signals == 0:
                print(f"\n  ⚠  Primary gate passed but supporting conditions blocked entry")
                print(f"     rte_min_supporting={cfg.get('rte_min_supporting',1)} — need at least this many of [rmi, vol, macd]")
            print()

        # Regime filter: for SQQQ (or any configured ticker), restrict entry dates
        ticker_regime_dates = None
        regime_cfg = cfg.get("regime_filters", {})
        if args.regime_filter and ticker in regime_cfg and regime_allowed_dates is not None:
            ticker_regime_dates = regime_allowed_dates
            print(f"  [{ticker}] Regime filter active — entries only on QQQ downtrend days")

        # Simulate trades (use ticker_cfg so per-ticker stop/target overrides apply)
        # --no-signal-sell flag OR signal_sell_enabled=False in config both disable signal exits
        _no_sig_sell = args.no_signal_sell or (not cfg.get("signal_sell_enabled", True))
        ticker_trades = simulate(bars, ticker_cfg, ticker, warmup_cutoff,
                                 no_signal_sell=_no_sig_sell,
                                 regime_dates=ticker_regime_dates)
        print(f"{len(ticker_trades)} trades simulated")

        # Attach float (non-blocking best-effort)
        try:
            from alpaca_dashboard import get_stock_info
            fi = get_stock_info(ticker)
            fm = fi.get("float_m", 0.0)
            for t in ticker_trades:
                t["float_m"] = fm
        except Exception:
            pass

        all_trades.extend(ticker_trades)
        stats_per_ticker[ticker] = compute_stats(ticker_trades)

        # Per-ticker console summary
        s = stats_per_ticker[ticker]
        if s:
            wr = s.get("win_rate",0)
            print(f"  [{ticker}] Win {wr}%  AvgWin {s.get('avg_win_pct',0):+.1f}%  "
                  f"AvgLoss {s.get('avg_loss_pct',0):+.1f}%  "
                  f"AvgR {s.get('avg_r',0):+.2f}  "
                  f"Expectancy {s.get('expectancy',0):+.2f}%  "
                  f"TotalPnL {s.get('total_pnl',0):+.1f}%  "
                  f"MaxDD -{s.get('max_drawdown',0):.1f}%")
        print()

    if not all_trades:
        print("No trades generated.  Check tickers, date range, or parameters.")
        return

    # Sort all trades chronologically for combined equity curve
    all_trades.sort(key=lambda t: t["date"] + t["entry_ts"])
    combined_stats = compute_stats(all_trades)

    # ── Console combined summary ──────────────────────────────────────────
    cs = combined_stats
    print(f"\n{'='*60}")
    print(f"  COMBINED  ({cs['total_trades']} trades across {len(args.tickers)} tickers)")
    print(f"  Win rate:   {cs['win_rate']}%   ({cs['wins']}W / {cs['losses']}L)")
    print(f"  Avg winner: {cs['avg_win_pct']:+.2f}%  |  Avg loser: {cs['avg_loss_pct']:+.2f}%")
    print(f"  Avg R:      {cs['avg_r']:+.2f}  |  Expectancy: {cs['expectancy']:+.2f}% / trade")
    print(f"  Total P&L:  {cs['total_pnl']:+.1f}%  (compounded)")
    print(f"  Max drawdown: -{cs['max_drawdown']:.1f}%")

    if cs.get("by_streak"):
        print(f"\n  By streak:")
        for k in sorted(cs["by_streak"].keys()):
            v = cs["by_streak"][k]
            wr = v["wins"]/v["count"]*100 if v["count"] else 0
            print(f"    streak={k}: {v['count']} trades, {wr:.0f}% win, "
                  f"{v['total_pnl']/v['count']:+.2f}% avg")

    if cs.get("by_reason"):
        print(f"\n  By exit reason:")
        for k, v in sorted(cs["by_reason"].items(), key=lambda x: -x[1]["count"]):
            wr = v["wins"]/v["count"]*100 if v["count"] else 0
            print(f"    {k:<15}: {v['count']:>3} trades, {wr:>5.1f}% win, "
                  f"{v['total_pnl']/v['count']:+.2f}% avg")

    print(f"{'='*60}\n")

    # ── Write CSV ─────────────────────────────────────────────────────────
    ts_str  = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"backtest_{ts_str}.csv"
    if all_trades:
        fieldnames = list(all_trades[0].keys())
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(all_trades)
        print(f"  CSV saved → {csv_path}")

    # ── Write HTML report ─────────────────────────────────────────────────
    if not args.no_html:
        html_path = out_dir / f"backtest_{ts_str}.html"
        html = build_html_report(all_trades, stats_per_ticker,
                                  combined_stats, cfg, start_str, end_str)
        html_path.write_text(html, encoding="utf-8")
        print(f"  HTML saved → {html_path}")
        # Try to open in browser
        try:
            import webbrowser
            webbrowser.open(html_path.resolve().as_uri())
        except Exception:
            pass

    print()


if __name__ == "__main__":
    main()
