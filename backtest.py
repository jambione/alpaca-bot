#!/usr/bin/env python3
"""
Alpaca Trading Bot — Backtester
Reuses the live bot's signal engine on historical 1-minute bars.
"""

import sys, os, json, csv, argparse, textwrap
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

from config import load_config
from signals import compute_signals

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
except ImportError:
    print("ERROR: alpaca-py not installed. Run: pip install alpaca-py")
    sys.exit(1)

ET = ZoneInfo("America/New_York")

def fetch_bars_range(client, ticker: str, start: datetime, end: datetime, timeframe: str = "1Min") -> pd.DataFrame | None:
    tf_map = {
        "1Min": TimeFrame(1, TimeFrameUnit.Minute),
        "5Min": TimeFrame(5, TimeFrameUnit.Minute),
        "15Min": TimeFrame(15, TimeFrameUnit.Minute),
        "1Hour": TimeFrame(1, TimeFrameUnit.Hour),
    }
    tf = tf_map.get(timeframe, TimeFrame(1, TimeFrameUnit.Minute))

    warmup_start = start - timedelta(days=7)

    try:
        from alpaca.data.enums import DataFeed
        feed = DataFeed.IEX
    except Exception:
        feed = None

    try:
        req_kwargs = dict(
            symbol_or_symbols=ticker,
            timeframe=tf,
            start=warmup_start,
            end=end,
        )
        if feed is not None:
            req_kwargs["feed"] = feed
        req = StockBarsRequest(**req_kwargs)
        bars = client.get_stock_bars(req).df
        if bars is None or bars.empty:
            return None
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(ticker, level="symbol")
        bars = bars[["open", "high", "low", "close", "volume"]].copy()
        bars = bars.dropna(subset=["close"])
        if bars.index.tz is None:
            bars.index = bars.index.tz_localize("UTC")
        bars.index = bars.index.tz_convert(ET)
        return bars
    except Exception as e:
        print(f"  ERROR fetching {ticker}: {e}")
        return None


def simulate(df: pd.DataFrame, cfg: dict, ticker: str, warmup_cutoff: datetime,
             no_signal_sell: bool = False, regime_dates: set = None) -> list[dict]:
    """
    Simulate trades using the signal column from compute_signals.
    """
    stop_pct         = cfg.get("stop_loss_pct", 0.04)
    target_pct       = cfg.get("take_profit_pct", 0.15)
    trail_act_pct    = cfg.get("trail_activation_pct", 0.08)
    trail_pct        = cfg.get("trail_stop_pct", 0.03)
    partial_pct      = cfg.get("partial_exit_pct", 0.06)
    partial_qty_pct  = cfg.get("partial_exit_qty_pct", 0.50)
    no_buy_h, no_buy_m = cfg.get("no_new_buys_after", [15, 30])
    eod_h, eod_m       = cfg.get("eod_liquidate_at", [16, 0])

    trades = []
    position = None

    bars = list(df.iterrows())

    for idx, (ts, row) in enumerate(bars):
        price = float(row["close"])
        signal = str(row.get("signal", "HOLD"))

        # Manage open position
        if position is not None:
            entry = position["entry_price"]
            high_water = position["high_water"]
            pct_gain = (price - entry) / entry

            if price > high_water:
                position["high_water"] = price
                high_water = price

            exit_reason = None
            exit_price = price

            if ts.hour > eod_h or (ts.hour == eod_h and ts.minute >= eod_m):
                exit_reason = "EOD"
            elif price <= entry * (1 - stop_pct):
                exit_reason = "STOP"
                exit_price = entry * (1 - stop_pct)
            elif price >= entry * (1 + target_pct):
                exit_reason = "TARGET"
                exit_price = entry * (1 + target_pct)
            elif pct_gain >= trail_act_pct:
                trail_stop = high_water * (1 - trail_pct)
                if price <= trail_stop:
                    exit_reason = "TRAIL"
                    exit_price = trail_stop
            elif signal == "SELL" and not no_signal_sell:
                exit_reason = "SIGNAL_SELL"

            if not position.get("partial_done", False) and pct_gain >= partial_pct:
                position["partial_done"] = True
                position["partial_price"] = price
                position["partial_gain"] = round(pct_gain * 100, 2)

            if exit_reason:
                raw_pnl = (exit_price - entry) / entry
                if position.get("partial_done"):
                    partial_return = (position["partial_price"] - entry) / entry
                    remaining_return = (exit_price - entry) / entry
                    blended = (partial_qty_pct * partial_return + (1 - partial_qty_pct) * remaining_return)
                    final_pnl = blended
                else:
                    final_pnl = raw_pnl

                hold_mins = int((ts - position["entry_ts"]).total_seconds() / 60)

                trades.append({
                    "ticker": ticker,
                    "date": position["entry_ts"].strftime("%Y-%m-%d"),
                    "entry_ts": position["entry_ts"].strftime("%H:%M"),
                    "exit_ts": ts.strftime("%H:%M"),
                    "entry_price": round(entry, 4),
                    "exit_price": round(exit_price, 4),
                    "pnl_pct": round(final_pnl * 100, 2),
                    "pnl_r": round(final_pnl / stop_pct, 2),
                    "hold_mins": hold_mins,
                    "exit_reason": exit_reason,
                    "streak": 0,
                    "float_m": 0.0,
                    "rvol": float(row.get("rvol", 0)),
                    "rsi2": round(float(row.get("rmi", 50)), 1),
                    "rte_fast": 0.0,
                    "rte_slow": 0.0,
                    "partial_done": position.get("partial_done", False),
                    "partial_gain": position.get("partial_gain", ""),
                    "win": final_pnl > 0,
                })
                position = None

        # Entry
        if position is None and signal == "BUY":
            if ts < warmup_cutoff:
                continue
            if regime_dates is not None and ts.date() not in regime_dates:
                continue
            if ts.hour > no_buy_h or (ts.hour == no_buy_h and ts.minute >= no_buy_m):
                continue
            if idx + 1 >= len(bars):
                continue
            next_open = float(bars[idx + 1][1]["open"])

            position = {
                "entry_ts": bars[idx + 1][0],
                "entry_price": next_open,
                "high_water": next_open,
                "partial_done": False,
                "streak": 0,
                "float_m": 0.0,
                "rvol": float(row.get("rvol", 0)),
                "rsi2": round(float(row.get("rmi", 50)), 1),
                "rte_fast": 0.0,
                "rte_slow": 0.0,
            }

    # Force close any open position at end
    if position is not None:
        last_price = float(df.iloc[-1]["close"])
        last_ts = df.index[-1]
        pnl = (last_price - position["entry_price"]) / position["entry_price"]
        hold_mins = int((last_ts - position["entry_ts"]).total_seconds() / 60)
        trades.append({
            "ticker": ticker,
            "date": position["entry_ts"].strftime("%Y-%m-%d"),
            "entry_ts": position["entry_ts"].strftime("%H:%M"),
            "exit_ts": last_ts.strftime("%H:%M"),
            "entry_price": round(position["entry_price"], 4),
            "exit_price": round(last_price, 4),
            "pnl_pct": round(pnl * 100, 2),
            "pnl_r": round(pnl / stop_pct, 2),
            "hold_mins": hold_mins,
            "exit_reason": "END_OF_DATA",
            "streak": 0,
            "float_m": 0.0,
            "rvol": float(df.iloc[-1].get("rvol", 0)),
            "rsi2": round(float(df.iloc[-1].get("rmi", 50)), 1),
            "rte_fast": 0.0,
            "rte_slow": 0.0,
            "partial_done": position.get("partial_done", False),
            "partial_gain": position.get("partial_gain", ""),
            "win": pnl > 0,
        })

    return trades


def compute_stats(trades: list[dict]) -> dict:
    if not trades:
        return {}
    wins = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    pnls = [t["pnl_pct"] for t in trades]
    rs = [t["pnl_r"] for t in trades]

    win_rate = len(wins) / len(trades) * 100 if trades else 0
    avg_win = np.mean([t["pnl_pct"] for t in wins]) if wins else 0
    avg_loss = np.mean([t["pnl_pct"] for t in losses]) if losses else 0
    avg_r = np.mean(rs)
    expectancy = (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)

    equity = 1.0
    curve = []
    for t in trades:
        equity *= (1 + t["pnl_pct"] / 100)
        curve.append(round(equity, 4))

    peak = 1.0
    max_dd = 0.0
    for e in curve:
        if e > peak:
            peak = e
        dd = (peak - e) / peak
        if dd > max_dd:
            max_dd = dd

    return {
        "total_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 1),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "avg_r": round(avg_r, 2),
        "expectancy": round(expectancy, 2),
        "total_pnl": round(sum(pnls), 2),
        "max_drawdown": round(max_dd * 100, 2),
        "equity_curve": curve,
    }


def main():
    parser = argparse.ArgumentParser(description="Backtest momentum strategy")
    parser.add_argument("--tickers", nargs="+", required=True)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--stop", type=float, default=None)
    parser.add_argument("--target", type=float, default=None)
    parser.add_argument("--out", type=str, default="./backtest_results")
    parser.add_argument("--no-html", action="store_true")
    args = parser.parse_args()

    cfg = load_config() if 'load_config' in globals() else {}

    if args.stop is not None:
        cfg["stop_loss_pct"] = args.stop / 100
    if args.target is not None:
        cfg["take_profit_pct"] = args.target / 100

    now_et = datetime.now(ET)
    if args.end:
        end_dt = datetime.fromisoformat(args.end).replace(hour=23, minute=59, tzinfo=ET)
    else:
        end_dt = now_et
    if args.start:
        start_dt = datetime.fromisoformat(args.start).replace(hour=0, minute=0, tzinfo=ET)
    else:
        start_dt = end_dt - timedelta(days=args.days)

    print(f"\n{'='*60}")
    print(f"  Backtest: {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}")
    print(f"  Tickers:  {', '.join(args.tickers)}")
    print(f"  Stop: {cfg.get('stop_loss_pct',0.04)*100:.0f}%  Target: {cfg.get('take_profit_pct',0.15)*100:.0f}%")
    print(f"{'='*60}\n")

    api_key = cfg.get("api_key")
    secret = cfg.get("secret_key")
    if not api_key or not secret:
        print("ERROR: API key/secret not found in config.")
        sys.exit(1)

    client = StockHistoricalDataClient(api_key, secret)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_trades = []
    stats_per_ticker = {}

    for ticker in args.tickers:
        ticker = ticker.upper()
        print(f"  [{ticker}] Fetching bars …", end=" ", flush=True)

        bars = fetch_bars_range(client, ticker, start_dt, end_dt)
        if bars is None or bars.empty:
            print("NO DATA — skipped")
            stats_per_ticker[ticker] = {}
            continue

        print(f"{len(bars):,} bars received → ", end="", flush=True)

        warmup_cutoff = start_dt

        try:
            bars = compute_signals(bars, cfg)
            n_signals = int((bars["signal"] == "BUY").sum())
            print(f"{n_signals} BUY signals → ", end="", flush=True)
        except Exception as e:
            print(f"signal error: {e} — skipped")
            stats_per_ticker[ticker] = {}
            continue

        ticker_trades = simulate(bars, cfg, ticker, warmup_cutoff)
        print(f"{len(ticker_trades)} trades simulated")

        all_trades.extend(ticker_trades)
        stats_per_ticker[ticker] = compute_stats(ticker_trades)

        s = stats_per_ticker[ticker]
        if s:
            print(f"  [{ticker}] Win {s.get('win_rate',0)}%  AvgWin {s.get('avg_win_pct',0):+.1f}%  "
                  f"AvgLoss {s.get('avg_loss_pct',0):+.1f}%  Expectancy {s.get('expectancy',0):+.2f}%  "
                  f"TotalPnL {s.get('total_pnl',0):+.1f}%  MaxDD -{s.get('max_drawdown',0):.1f}%")

    if not all_trades:
        print("No trades generated.")
        return

    combined_stats = compute_stats(all_trades)

    cs = combined_stats
    print(f"\n{'='*60}")
    print(f"  COMBINED  ({cs['total_trades']} trades)")
    print(f"  Win rate:   {cs['win_rate']}%")
    print(f"  Avg winner: {cs['avg_win_pct']:+.2f}%  |  Avg loser: {cs['avg_loss_pct']:+.2f}%")
    print(f"  Expectancy: {cs['expectancy']:+.2f}% / trade")
    print(f"  Total P&L:  {cs['total_pnl']:+.1f}%")
    print(f"  Max drawdown: -{cs['max_drawdown']:.1f}%")
    print(f"{'='*60}\n")

    # Save CSV
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"backtest_{ts_str}.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=all_trades[0].keys())
        w.writeheader()
        w.writerows(all_trades)
    print(f"  CSV saved → {csv_path}")

    print("Done.")


if __name__ == "__main__":
    main()