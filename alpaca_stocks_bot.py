#!/usr/bin/env python3
"""
=============================================================
  Alpaca Momentum Trading Bot  |  Paper Trading Mode
=============================================================
  Strategy : Custom High-Momentum with %R Exhaustion (red side)
  Exit     : Red Down Arrow Exhaustion (paramount)
  Version  : v2.1 – Improved %R Box Detection + Detailed Logging
=============================================================
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from trade_logger import init_trade_log, log_trade

ET = ZoneInfo("America/New_York")

import pandas as pd
import numpy as np

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
CONFIG = {
    "api_key": os.getenv("ALPACA_API_KEY", "PKZCOG7Y6DKF7JZAOXAEB2AWC4"),
    "secret_key": os.getenv("ALPACA_SECRET_KEY", "GVj7Eak1MoCrWeV9sTrhRxgTqMagnfZ6YJ7NcpLK9R56"),

    "tickers": ["AIFF","AIRS","AVTX","BIAF","BNGO","EDBL","GMEX","PAVS","PTLE","RIME","SER","STIM","SUNE","TOVX","TSSI","UGRO","ANNA"],

    # Technical Parameters
    "ema_short": 9,
    "ema_long": 21,
    "rsi_period": 14,

    # Risk & Position Management
    "position_size_pct": 0.05,
    "max_positions": 8,
    "stop_loss_pct": 0.03,
    "take_profit_pct": 0.06,

    # Scanning
    "scan_interval_sec": 60,
    "bar_timeframe": "15Min",
    "bar_count": 300,

    # Strategy Toggles
    "use_ema_crossover_buy": True,
    "use_low_float_high_volume": True,
    "use_rte_exhaustion": True,
    "use_cm_rsi_lower": True,
    "use_volume_trending_up": True,

    # %R Exhaustion Settings
    "rte_side": "red",
    "rte_threshold": 20,
    "rte_min_boxes": 1,           # You can increase to 2 if you want stricter

    "high_volume_multiplier": 1.3,
    "cm_rsi_threshold": 35,       # Loosened from 20 → 35 for testing (tune as needed)

    # Price filters
    "min_price": 0.50,
    "max_price": 50.0,

    # Safety
    "daily_loss_limit_pct": 5.0,
}

# ─────────────────────────────────────────────
#  LOGGING & TRADE LOGGER
# ─────────────────────────────────────────────
LOG_FILE = "alpaca_bot.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("MomentumBot")

# ─────────────────────────────────────────────
#  INDICATORS
# ─────────────────────────────────────────────
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def williams_pr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    highest = high.rolling(window=length).max()
    lowest = low.rolling(window=length).min()
    return 100 * (close - highest) / (highest - lowest)

def compute_percent_r_exhaustion(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Improved version to better match visual red boxes/arrows on chart"""
    df = df.copy()
    
    s_pr = williams_pr(df["high"], df["low"], df["close"], 21)
    l_pr = williams_pr(df["high"], df["low"], df["close"], 112)

    s_percentR = s_pr.ewm(span=7, adjust=False).mean()
    l_percentR = l_pr.ewm(span=3, adjust=False).mean()

    threshold = cfg["rte_threshold"]

    if cfg.get("rte_side", "red").lower() == "red":
        extreme = (s_percentR >= -threshold) & (l_percentR >= -threshold)
    else:
        extreme = (s_percentR <= -100 + threshold) & (l_percentR <= -100 + threshold)

    # Red Down Arrow = exit from extreme zone
    df["rte_red_down_arrow"] = (~extreme) & extreme.shift(1).fillna(False)
    df["rte_extreme"] = extreme
    
    # Count completed boxes
    df["rte_boxes_completed"] = df["rte_red_down_arrow"].cumsum().fillna(0).astype(int)
    
    # Recent boxes (more relevant for current momentum) - last 50 bars (~12.5 hours on 15min)
    df["rte_boxes_recent"] = df["rte_red_down_arrow"].rolling(window=50, min_periods=1).sum().astype(int)

    return df

def compute_cm_rsi_lower(df: pd.DataFrame, threshold: int = 20) -> pd.DataFrame:
    df = df.copy()
    delta = df["close"].diff()
    up = delta.clip(lower=0).ewm(alpha=1/2, adjust=False).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1/2, adjust=False).mean()
    
    rsi2 = np.where(down == 0, 100, 
                    np.where(up == 0, 0, 
                             100 - (100 / (1 + up / down))))
    
    df["cm_rsi"] = pd.Series(rsi2, index=df.index)
    df["cm_rsi_rising"] = df["cm_rsi"] > df["cm_rsi"].shift(1)
    return df

def compute_obv_oscillator(df: pd.DataFrame, length: int = 20) -> pd.DataFrame:
    df = df.copy()
    change = df["close"].diff()
    obv_series = np.where(change > 0, df["volume"], np.where(change < 0, -df["volume"], 0))
    obv = pd.Series(obv_series, index=df.index).cumsum()
    ema_obv = obv.ewm(span=length, adjust=False).mean()
    df["obv_osc"] = obv - ema_obv
    df["volume_trending_up"] = df["obv_osc"] > 0
    return df

def compute_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()

    df["ema_short"] = ema(df["close"], cfg["ema_short"])
    df["ema_long"] = ema(df["close"], cfg["ema_long"])
    df["rsi"] = rsi(df["close"], cfg["rsi_period"])

    df["cross_up"] = (df["ema_short"] > df["ema_long"]) & (df["ema_short"].shift(1) <= df["ema_long"].shift(1))
    df["cross_down"] = (df["ema_short"] < df["ema_long"]) & (df["ema_short"].shift(1) >= df["ema_long"].shift(1))

    df = compute_percent_r_exhaustion(df, cfg)
    df = compute_cm_rsi_lower(df, cfg["cm_rsi_threshold"])
    df = compute_obv_oscillator(df)

    vol_ma = df["volume"].rolling(20).mean()
    df["high_volume"] = df["volume"] > (vol_ma * cfg["high_volume_multiplier"])

    # Buy conditions - use bitwise OR to combine conditions
    buy_condition = pd.Series(False, index=df.index)
    if cfg.get("use_ema_crossover_buy", False):
        buy_condition = buy_condition | (df["cross_up"] & (df["rsi"] < 70))
    if cfg.get("use_low_float_high_volume", True):
        buy_condition = buy_condition | df["high_volume"]
    if cfg.get("use_rte_exhaustion", True):
        buy_condition = buy_condition | ((df["rte_boxes_completed"] >= cfg["rte_min_boxes"]) & df["rte_extreme"])
    if cfg.get("use_cm_rsi_lower", True):
        buy_condition = buy_condition | ((df["cm_rsi"] < cfg["cm_rsi_threshold"]) & df["cm_rsi_rising"])
    if cfg.get("use_volume_trending_up", True):
        buy_condition = buy_condition | df["volume_trending_up"]
    
    # Signal sells (EMA crossdown / RSI overbought) — disabled by default for exhaustion strategy
    if cfg.get("signal_sell_enabled", True):
        sell_condition = df["cross_down"] | (df["rsi"] > 75)
        df["signal"] = "HOLD"
        df.loc[buy_condition, "signal"] = "BUY"
        df.loc[sell_condition, "signal"] = "SELL"
    else:
        df["signal"] = "HOLD"
        df.loc[buy_condition, "signal"] = "BUY"

    return df

# ─────────────────────────────────────────────
#  TRADING WINDOW HELPERS
# ─────────────────────────────────────────────
def _et_hm() -> tuple:
    """Return current (hour, minute) in US/Eastern time."""
    n = datetime.now(ET)
    return (n.hour, n.minute)

def _is_pre_market_now(cfg: dict) -> bool:
    """True if pre-market is enabled and current ET time is before 9:30 AM."""
    if not cfg.get("pre_market_enabled", False):
        return False
    t = _et_hm()
    pre_start = tuple(cfg.get("pre_market_start", [4, 0]))
    return pre_start <= t < (9, 30)

def _trading_window_status(cfg: dict) -> dict:
    """
    Returns dict with:
      can_scan  : bot should run at all right now
      can_buy   : allowed to open new positions
      do_eod    : time to liquidate all positions now
    """
    t = _et_hm()
    eod      = tuple(cfg.get("eod_liquidate_at",  [16, 0]))
    no_buy   = tuple(cfg.get("no_new_buys_after", [15, 30]))
    pre_en   = cfg.get("pre_market_enabled", False)
    pre_start = tuple(cfg.get("pre_market_start", [4, 0]))

    # After EOD — no scanning
    if t >= eod:
        return {"can_scan": False, "can_buy": False, "do_eod": True}

    # Before pre-market start (or before 9:30 when pre-market disabled)
    earliest = pre_start if pre_en else (9, 30)
    if t < earliest:
        return {"can_scan": False, "can_buy": False, "do_eod": False}

    # Within window — can scan; new buys only before no_new_buys_after
    return {"can_scan": True, "can_buy": t < no_buy, "do_eod": False}


# ─────────────────────────────────────────────
#  ALPACA HELPERS
# ─────────────────────────────────────────────
def connect_alpaca(cfg: dict):
    try:
        from alpaca.trading.client import TradingClient
        from alpaca.data.historical import StockHistoricalDataClient
    except ImportError:
        raise SystemExit("❌ Please install: pip install alpaca-py pandas numpy")

    trading = TradingClient(cfg["api_key"], cfg["secret_key"], paper=True)
    data = StockHistoricalDataClient(cfg["api_key"], cfg["secret_key"])

    account = trading.get_account()
    log.info(f"[OK] Connected to Alpaca Paper | Equity: ${float(account.equity):,.2f}")
    return trading, data

def get_latest_trade_price(data_client, ticker: str) -> float | None:
    try:
        from alpaca.data.requests import StockLatestQuoteRequest
        from alpaca.data.enums import DataFeed
        # Use latest quote (bid/ask midpoint) — works in pre-market via IEX feed
        req = StockLatestQuoteRequest(symbol_or_symbols=ticker, feed=DataFeed.IEX)
        latest = data_client.get_latest_quote(req)
        quote = latest[ticker] if isinstance(latest, dict) else latest
        bid = float(quote.bid_price or 0)
        ask = float(quote.ask_price or 0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2, 4)
        if ask > 0:
            return ask
        if bid > 0:
            return bid
        return None
    except Exception as e:
        log.warning(f"Could not get live price for {ticker}: {e}")
        return None

def fetch_bars(data_client, ticker: str, cfg: dict) -> pd.DataFrame | None:
    try:
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
        from alpaca.data.enums import DataFeed

        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(15, TimeFrameUnit.Minute),
            start=datetime.now(timezone.utc) - timedelta(days=30),
            limit=cfg["bar_count"],
            adjustment="split",
            feed=DataFeed.IEX
        )
        bars = data_client.get_stock_bars(req).df

        if bars.empty:
            return None

        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(ticker, level="symbol")

        df = bars[["open", "high", "low", "close", "volume"]].copy().dropna(subset=["close"])
        return df
    except Exception as e:
        log.error(f"Error fetching bars for {ticker}: {e}")
        return None

def get_open_positions(trading_client) -> dict:
    positions = {}
    try:
        for pos in trading_client.get_all_positions():
            positions[pos.symbol] = {
                "qty": int(float(pos.qty)),
                "entry_price": float(pos.avg_entry_price)
            }
        return positions
    except Exception:
        return {}

def get_portfolio_value(trading_client) -> float:
    try:
        return float(trading_client.get_account().equity)
    except Exception:
        return 100_000.0

def can_trade_today(portfolio_value: float, day_start_equity: float, cfg: dict) -> bool:
    """Returns False if today's drawdown exceeds max_daily_loss_pct from config."""
    if day_start_equity <= 0:
        return True
    loss_pct = (portfolio_value - day_start_equity) / day_start_equity * 100
    limit = cfg.get("max_daily_loss_pct", 0.05) * 100  # config stores as 0.05 = 5%
    if loss_pct < -limit:
        log.warning(f"⛔ DAILY LOSS LIMIT HIT: {loss_pct:.2f}% (limit: -{limit:.1f}%)  — No new buys today.")
        return False
    return True

# ─────────────────────────────────────────────
#  ORDER FUNCTIONS
# ─────────────────────────────────────────────
def place_buy(trading_client, ticker: str, qty: int, price: float, cfg: dict = None) -> bool:
    if qty < 1:
        return False
    cfg = cfg or {}
    try:
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        if _is_pre_market_now(cfg):
            # Pre-market: must use limit order with extended_hours=True
            offset = cfg.get("pre_market_limit_offset_pct", 0.002)
            limit_price = round(price * (1 + offset), 2)
            req = LimitOrderRequest(
                symbol=ticker, qty=qty, side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                extended_hours=True
            )
            log.info(f"  [PRE-MKT BUY]  {qty} x {ticker} @ limit ${limit_price:.2f} (mid ${price:.2f})")
        else:
            req = MarketOrderRequest(symbol=ticker, qty=qty, side=OrderSide.BUY, time_in_force=TimeInForce.DAY)
            log.info(f"  [BUY]  {qty} x {ticker} @ ~${price:.2f}")

        trading_client.submit_order(req)
        log_trade(ticker, "BUY", qty, price, reason="SIGNAL")
        return True
    except Exception as e:
        log.error(f"BUY order failed for {ticker}: {e}")
        return False

def place_sell(trading_client, ticker: str, qty: int, price: float, reason: str = "SIGNAL", cfg: dict = None) -> bool:
    cfg = cfg or {}
    try:
        from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce

        if _is_pre_market_now(cfg):
            # Pre-market: limit order 0.1% below mid to maximize fill probability
            limit_price = round(price * 0.999, 2)
            req = LimitOrderRequest(
                symbol=ticker, qty=qty, side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                limit_price=limit_price,
                extended_hours=True
            )
            log.info(f"  [PRE-MKT SELL] {qty} x {ticker} @ limit ${limit_price:.2f}  ({reason})")
        else:
            req = MarketOrderRequest(symbol=ticker, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY)
            log.info(f"  [SELL] {qty} x {ticker} @ ~${price:.2f}  ({reason})")

        trading_client.submit_order(req)
        return True
    except Exception as e:
        log.error(f"SELL order failed for {ticker}: {e}")
        return False

# ─────────────────────────────────────────────
#  MAIN BOT LOOP
# ─────────────────────────────────────────────
def run_bot(trading_client, data_client, cfg: dict):
    log.info("=" * 85)
    log.info("   ALPACA HIGH-MOMENTUM BOT  —  PAPER TRADING  [v2.1]")
    log.info(f"   RTE Side: {cfg['rte_side'].upper()} | Red Down Arrow Exit | CM_RSI < {cfg['cm_rsi_threshold']}")
    log.info("=" * 85)

    init_trade_log()
    entry_prices: dict = {}
    peak_prices: dict = {}    # Tracks highest price seen while in position (for trailing stop)

    # Daily loss circuit breaker tracking
    day_start_equity: float = 0.0
    last_trading_date: str  = ""

    eod_liquidated_today: str = ""  # track which dates we've already liquidated

    while True:
        try:
            now        = datetime.now(ET)
            now_str    = now.strftime("%H:%M:%S")
            today_str  = now.strftime("%Y-%m-%d")
            win        = _trading_window_status(cfg)
            pre_mkt    = _is_pre_market_now(cfg)

            log.info(f"\n--- Scan started at {now_str} ET {'[PRE-MARKET]' if pre_mkt else ''} ---")

            # ── EOD liquidation ──────────────────────────────────────────
            if win["do_eod"] and eod_liquidated_today != today_str:
                open_positions = get_open_positions(trading_client)
                if open_positions:
                    log.info("🔔 EOD — liquidating all open positions")
                    for t, pos in open_positions.items():
                        lp = get_latest_trade_price(data_client, t) or pos["entry_price"]
                        place_sell(trading_client, t, pos["qty"], lp, "EOD", cfg)
                        entry_prices.pop(t, None)
                        peak_prices.pop(t, None)
                eod_liquidated_today = today_str
                time.sleep(cfg["scan_interval_sec"])
                continue

            # ── Outside trading window — wait quietly ────────────────────
            if not win["can_scan"]:
                t_now = _et_hm()
                pre_start = tuple(cfg.get("pre_market_start", [4, 0])) if cfg.get("pre_market_enabled") else (9, 30)
                log.info(f"💤 Outside trading window (ET {t_now[0]:02d}:{t_now[1]:02d}) — waiting for {pre_start[0]:02d}:{pre_start[1]:02d} ET")
                time.sleep(60)
                continue

            portfolio_value = get_portfolio_value(trading_client)
            open_positions  = get_open_positions(trading_client)

            # Reset daily start equity at the top of each new trading day
            if today_str != last_trading_date:
                day_start_equity  = portfolio_value
                last_trading_date = today_str
                log.info(f"📅 New trading day — day start equity: ${day_start_equity:,.2f}")

            log.info(f"Equity: ${portfolio_value:,.2f} | Day start: ${day_start_equity:,.2f} | Open Positions: {len(open_positions)}")

            if not can_trade_today(portfolio_value, day_start_equity, cfg):
                log.info("🚫 Trading paused — daily loss limit reached. Holding existing positions only.")
                time.sleep(cfg["scan_interval_sec"])
                continue

            if not win["can_buy"]:
                log.info(f"⏰ Past no_new_buys_after — managing open positions only")

            active_positions = set(open_positions.keys())

            for ticker in cfg["tickers"]:
                log.info(f"  → Checking {ticker}")

                df = fetch_bars(data_client, ticker, cfg)
                if df is None or len(df) < 150:
                    log.info(f"    Skipped {ticker} - insufficient bars")
                    continue

                live_price = get_latest_trade_price(data_client, ticker)
                # ETF tickers bypass price range filter (SQQQ, UVXY etc. can be any price)
                is_etf = ticker in cfg.get("etf_tickers", [])
                if live_price is None:
                    continue
                if not is_etf and not (cfg["min_price"] <= live_price <= cfg["max_price"]):
                    continue

                # Smart reverse split adjustment
                last_close = df["close"].iloc[-1]
                if abs(live_price - last_close) / last_close > 0.08:
                    factor = live_price / last_close
                    df[["open", "high", "low", "close"]] *= factor
                    log.info(f"  {ticker} adjusted by {factor:.2f}x (live=${live_price:.3f})")

                price = live_price

                # Apply per-ticker config overrides (e.g. rte_min_boxes per ticker)
                ticker_cfg = dict(cfg)
                overrides = cfg.get("ticker_overrides", {}).get(ticker, {})
                if overrides:
                    ticker_cfg.update(overrides)
                    log.debug(f"  [{ticker}] overrides applied: {overrides}")

                df = compute_signals(df, ticker_cfg)
                latest = df.iloc[-1]

                # Detailed logging for debugging
                log.info(
                    f"  {ticker} ${price:.3f} | "
                    f"CM_RSI={latest.get('cm_rsi', np.nan):.1f}{'↑' if latest.get('cm_rsi_rising', False) else '↓'} | "
                    f"RTE_boxes={int(latest.get('rte_boxes_completed', 0))} "
                    f"(recent={int(latest.get('rte_boxes_recent', 0))}) | "
                    f"RedArrow={bool(latest.get('rte_red_down_arrow', False))} | "
                    f"Extreme={bool(latest.get('rte_extreme', False))} | "
                    f"VolHigh={bool(latest.get('high_volume', False))} | "
                    f"Signal={latest.get('signal', 'HOLD')}"
                )

                in_position = ticker in active_positions
                qty_held = open_positions.get(ticker, {}).get("qty", 0)
                entry_price = entry_prices.get(ticker) or open_positions.get(ticker, {}).get("entry_price")

                # Red Down Arrow Exit - Highest Priority
                if in_position and latest.get("rte_red_down_arrow", False) and entry_price:
                    if place_sell(trading_client, ticker, qty_held, price, "EXHAUSTION", ticker_cfg):
                        log_trade(ticker, "EXHAUSTION", qty_held, price, entry_price, "EXHAUSTION")
                        entry_prices.pop(ticker, None)
                        peak_prices.pop(ticker, None)
                    continue

                # Normal position management
                if in_position and entry_price:
                    pct_change = (price - entry_price) / entry_price

                    # Update peak price for trailing stop
                    if pct_change > 0:
                        peak_prices[ticker] = max(peak_prices.get(ticker, price), price)

                    if pct_change <= -ticker_cfg["stop_loss_pct"]:
                        if place_sell(trading_client, ticker, qty_held, price, "STOP", ticker_cfg):
                            log_trade(ticker, "STOP", qty_held, price, entry_price, "STOP")
                            entry_prices.pop(ticker, None)
                            peak_prices.pop(ticker, None)
                        continue

                    # Trailing stop — activates after trail_activation_pct gain, then protects
                    trail_activation = ticker_cfg.get("trail_activation_pct", 0.05)
                    trail_distance = ticker_cfg.get("trail_stop_pct", 0.02)
                    peak = peak_prices.get(ticker, price)
                    peak_gain = (peak - entry_price) / entry_price
                    if peak_gain >= trail_activation:
                        trail_trigger = peak * (1 - trail_distance)
                        if price <= trail_trigger:
                            if place_sell(trading_client, ticker, qty_held, price, "TRAIL", ticker_cfg):
                                log_trade(ticker, "TRAIL", qty_held, price, entry_price, "TRAIL")
                                entry_prices.pop(ticker, None)
                                peak_prices.pop(ticker, None)
                            continue

                    if pct_change >= ticker_cfg["take_profit_pct"]:
                        if place_sell(trading_client, ticker, qty_held, price, "TP", ticker_cfg):
                            log_trade(ticker, "TP", qty_held, price, entry_price, "TP")
                            entry_prices.pop(ticker, None)
                            peak_prices.pop(ticker, None)
                        continue

                # Signal-based trading
                signal = latest.get("signal")
                if signal == "SELL" and in_position and entry_price and ticker_cfg.get("signal_sell_enabled", True):
                    if place_sell(trading_client, ticker, qty_held, price, "SIGNAL", ticker_cfg):
                        log_trade(ticker, "SIGNAL", qty_held, price, entry_price, "SIGNAL")
                        entry_prices.pop(ticker, None)
                        peak_prices.pop(ticker, None)

                elif signal == "BUY" and not in_position and win["can_buy"]:
                    # Regime filter: check reference ticker trend before entry
                    regime_cfg = cfg.get("regime_filters", {})
                    if ticker in regime_cfg:
                        ref_ticker = regime_cfg[ticker]["reference"]
                        try:
                            from alpaca.data.requests import StockBarsRequest
                            from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
                            from alpaca.data.enums import DataFeed
                            from datetime import timedelta
                            rf_req = StockBarsRequest(
                                symbol_or_symbols=ref_ticker,
                                timeframe=TimeFrame(1, TimeFrameUnit.Day),
                                start=datetime.now() - timedelta(days=35),
                                end=datetime.now(),
                                feed=DataFeed.IEX
                            )
                            rf_bars = data_client.get_stock_bars(rf_req).df
                            if isinstance(rf_bars.index, pd.MultiIndex):
                                rf_bars = rf_bars.xs(ref_ticker, level="symbol")
                            rf_bars["ema20"] = rf_bars["close"].ewm(span=20, adjust=False).mean()
                            last_ref = rf_bars.iloc[-1]
                            ref_in_downtrend = last_ref["close"] < last_ref["ema20"]
                            if not ref_in_downtrend:
                                log.info(f"  [{ticker}] Regime filter: {ref_ticker} above EMA20 — skipping entry")
                                continue
                            log.info(f"  [{ticker}] Regime filter: {ref_ticker} below EMA20 ✓ entry allowed")
                        except Exception as e:
                            log.warning(f"  Regime filter check failed for {ticker}: {e} — allowing entry")

                    if len(active_positions) >= cfg["max_positions"]:
                        log.info(f"  Max positions reached. Skipping BUY on {ticker}")
                        continue

                    qty = int((portfolio_value * cfg["position_size_pct"]) / price)
                    if qty >= 1:
                        if place_buy(trading_client, ticker, qty, price, cfg):
                            entry_prices[ticker] = price
                            peak_prices[ticker] = price  # Initialize peak at entry

            log.info(f"  Scan complete. Sleeping {cfg['scan_interval_sec']}s...\n")
            time.sleep(cfg["scan_interval_sec"])

        except Exception as e:
            log.error(f"Error in main loop: {e}", exc_info=True)
            time.sleep(30)

if __name__ == "__main__":
    try:
        trading_client, data_client = connect_alpaca(CONFIG)
        run_bot(trading_client, data_client, CONFIG)
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
    except Exception as e:
        log.critical(f"Fatal error: {e}", exc_info=True)