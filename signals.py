"""
signals.py — Indicator calculations and signal engine.

All pure-math functions: no Alpaca, no FastAPI, no side effects.
Imported by both alpaca_dashboard.py (live bot) and backtest.py.
"""

import numpy as np
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# ═══════════════════════════════════════════════════════════
#  BASE INDICATORS
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

def williams_pr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    """
    Williams Percent Range.
    Range: 0 (overbought) to -100 (oversold).
    Classic thresholds: overbought > -20, oversold < -80.
    """
    hh = high.rolling(length).max()
    ll = low.rolling(length).min()
    return -100 * (hh - close) / (hh - ll).replace(0, np.nan)


# ═══════════════════════════════════════════════════════════
#  STRATEGY INDICATORS
# ═══════════════════════════════════════════════════════════

def compute_percent_r_exhaustion(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Williams %R Exhaustion — two-line version matching the TradingView indicator.

    White (fast) line : EMA-7 of W%R(21)
    Blue  (slow) line : EMA-3 of W%R(112)

    extreme  = BOTH lines are simultaneously >= -threshold (default -20).
    reversal = one bar after extreme ends — entry signal.
    """
    df = df.copy()

    s_pr = williams_pr(df["high"], df["low"], df["close"], 21)
    l_pr = williams_pr(df["high"], df["low"], df["close"], 112)
    s_percentR = s_pr.ewm(span=7, adjust=False).mean()   # fast / white line
    l_percentR = l_pr.ewm(span=3, adjust=False).mean()   # slow / blue  line

    avg_ma       = cfg.get("rte_avg_ma", 3)
    avg_percentR = (s_percentR + l_percentR) / 2
    final        = avg_percentR.ewm(span=avg_ma, adjust=False).mean()

    threshold = abs(cfg.get("rte_threshold", 20))
    side      = cfg.get("rte_side", "red").lower()

    if side == "red":
        extreme = (s_percentR >= -threshold) & (l_percentR >= -threshold)
    else:
        extreme = (s_percentR <= (-100 + threshold)) & (l_percentR <= (-100 + threshold))

    reversal = (~extreme) & extreme.shift(1).fillna(False)
    boxes    = reversal.cumsum().fillna(0).astype(int)

    # ── Streak counter ────────────────────────────────────────────────────────
    # streak = 1 → first box fired  → ticker eligible for On Deck
    # streak = 2 → second box fired → ticker eligible for Buy
    # Resets when BOTH %R lines fall below the failure threshold.
    _fail_thr  = cfg.get("precheck_threshold", -40)
    setup_fail = (s_percentR < _fail_thr) & (l_percentR < _fail_thr)
    streak_arr = np.zeros(len(df), dtype=int)
    _count     = 0
    for _i in range(len(df)):
        if bool(setup_fail.iloc[_i]):
            _count = 0
        if bool(reversal.iloc[_i]):
            _count += 1
        streak_arr[_i] = _count
    boxes_streak = pd.Series(streak_arr, index=df.index)

    df["rte_fast"]            = s_percentR
    df["rte_slow"]            = l_percentR
    df["rte_composite"]       = final
    df["rte_extreme"]         = extreme
    df["rte_reversal"]        = reversal
    df["rte_boxes_completed"] = boxes
    df["rte_boxes_streak"]    = boxes_streak
    return df


def compute_rmi(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    ChrisMoody / Larry Connors RSI-2 Strategy.
    Bullish when: close > SMA(200) AND close < SMA(5) AND RSI-2 < oversold threshold.
    """
    oversold = cfg.get("rmi_oversold", 10)
    ma_fast  = cfg.get("rmi_ma_fast",   5)
    ma_slow  = cfg.get("rmi_ma_slow", 200)

    delta = df["close"].diff()
    gain  = delta.clip(lower=0).ewm(alpha=0.5, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=0.5, adjust=False).mean()
    rsi2  = np.where(
        loss == 0, 100,
        np.where(gain == 0, 0, 100 - (100 / (1 + gain / loss)))
    )
    df["rmi"]         = pd.Series(rsi2, index=df.index)
    df["rmi_ma_fast"] = df["close"].rolling(ma_fast).mean()
    df["rmi_ma_slow"] = df["close"].rolling(ma_slow).mean()

    above_slow = df["close"] > df["rmi_ma_slow"]
    below_fast = df["close"] < df["rmi_ma_fast"]
    deeply_os  = df["rmi"]   < oversold
    df["rmi_signal"] = above_slow & below_fast & deeply_os
    return df


def compute_obv_oscillator(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    On Balance Volume Oscillator = OBV − EMA(OBV, length).
    Positive oscillator → buying pressure dominates.
    """
    length = cfg.get("obv_length", 20)

    direction    = np.sign(df["close"].diff()).fillna(0)
    df["obv"]    = (direction * df["volume"]).cumsum()
    df["obv_ema"] = ema(df["obv"], length)
    df["obv_osc"] = df["obv"] - df["obv_ema"]

    df["obv_bull"]   = (df["obv_osc"].shift(1) < 0) & (df["obv_osc"] >= 0)
    df["obv_bear"]   = (df["obv_osc"].shift(1) > 0) & (df["obv_osc"] <= 0)
    df["obv_rising"] = (df["obv_osc"] > 0) & (df["obv_osc"] > df["obv_osc"].shift(1))
    return df


def compute_volume_trending_up(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Detects whether volume is in a meaningful uptrend.
    vol_trend_up: short-term volume MA is above long-term volume MA.
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


def compute_macd(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    MACD = EMA(fast) − EMA(slow).  Signal = EMA(MACD, signal_period).
    macd_bull: MACD crosses UP through signal line.
    macd_bear: MACD crosses DOWN through signal line.
    """
    fast = cfg.get("macd_fast",   12)
    slow = cfg.get("macd_slow",   26)
    sig  = cfg.get("macd_signal",  9)

    df["macd_line"]        = ema(df["close"], fast) - ema(df["close"], slow)
    df["macd_signal_line"] = ema(df["macd_line"], sig)
    df["macd_hist"]        = df["macd_line"] - df["macd_signal_line"]
    df["macd_bull"] = (
        (df["macd_line"] >  df["macd_signal_line"]) &
        (df["macd_line"].shift(1) <= df["macd_signal_line"].shift(1))
    )
    df["macd_bear"] = (
        (df["macd_line"] <  df["macd_signal_line"]) &
        (df["macd_line"].shift(1) >= df["macd_signal_line"].shift(1))
    )
    return df


# ═══════════════════════════════════════════════════════════
#  MAIN SIGNAL ROUTER
# ═══════════════════════════════════════════════════════════

def compute_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Route to the correct signal strategy based on cfg["strategy"].

    "ema_crossover" — EMA 8/21 + RSI range + volume surge + optional VWAP
    "macd"          — Pure MACD crossover + volume surge
    "exhaustion"    — %R Exhaustion (red zone) + CM RSI + volume + MACD (default)
    """
    df = df.copy()
    strategy = cfg.get("strategy", "exhaustion")

    # ── Common base indicators ───────────────────────────────
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
        df.loc[df["macd_bull"] & df["vol_surge"], "signal"] = "BUY"
        df.loc[df["macd_bear"],                   "signal"] = "SELL"

    # ── Exhaustion strategy (default) ────────────────────────
    else:
        df = compute_percent_r_exhaustion(df, cfg)
        df = compute_rmi(df, cfg)
        df = compute_obv_oscillator(df, cfg)
        df = compute_volume_trending_up(df, cfg)
        df = compute_macd(df, cfg)

        min_boxes   = cfg.get("rte_min_boxes",     1)
        entry_win   = cfg.get("rte_entry_window",  3)
        min_support = cfg.get("rte_min_supporting",1)

        rte_valid = df["rte_reversal"] if entry_win <= 1 else (
            df["rte_reversal"]
              .rolling(window=entry_win, min_periods=1)
              .max().fillna(0).astype(bool)
        )
        primary = rte_valid & (df["rte_boxes_streak"] >= min_boxes)

        support_scores = pd.Series(0, index=df.index)
        if cfg.get("use_rmi", True):
            support_scores += df["rmi_signal"].astype(int)
        if cfg.get("use_volume_trending_up", True):
            support_scores += (df["vol_trend_up"] | df["vol_surge"]).astype(int)
        if cfg.get("use_macd", True):
            support_scores += (df["macd_line"] > df["macd_signal_line"]).astype(int)

        if cfg.get("use_rte_exhaustion", True):
            combined_buy = primary & (support_scores >= min_support)
        else:
            combined_buy = df["cross_up"] & df["vol_surge"]

        df.loc[combined_buy, "signal"] = "BUY"

        sell_conds = [df["cross_down"]]
        if cfg.get("use_macd", True):
            sell_conds.append(df["macd_bear"])
        combined_sell = sell_conds[0]
        for c in sell_conds[1:]:
            combined_sell = combined_sell | c
        df.loc[combined_sell, "signal"] = "SELL"

    # RSI always present (used by pyramid check)
    if "rsi" not in df.columns:
        df["rsi"] = rsi(df["close"], cfg.get("rsi_period", 14))

    # ── ATR (used for position sizing) ──────────────────────
    _atr_p = cfg.get("atr_period", 14)
    _tr_hl  = df["high"] - df["low"]
    _tr_hc  = (df["high"] - df["close"].shift(1)).abs()
    _tr_lc  = (df["low"]  - df["close"].shift(1)).abs()
    df["atr"] = pd.concat([_tr_hl, _tr_hc, _tr_lc], axis=1).max(axis=1).ewm(
        span=_atr_p, adjust=False).mean()

    return df


# ═══════════════════════════════════════════════════════════
#  RVOL
# ═══════════════════════════════════════════════════════════

def calc_rvol(df: pd.DataFrame, avg_daily_vol: int = 0) -> float:
    """
    Calculate RVOL using bar data only — no yfinance required.
    Compares today's accumulated volume to the average volume over the same
    number of bars on previous trading days in the same DataFrame.
    """
    try:
        now_et = datetime.now(ET)
        today  = now_et.date()

        dates_series = pd.Series([i.date() for i in df.index], index=df.index)
        today_mask   = dates_series == today
        today_bars   = df.loc[today_mask]
        today_vol    = float(today_bars["volume"].sum())

        if today_vol == 0:
            return 0.0

        n_today    = len(today_bars)
        prev_dates = sorted(d for d in dates_series.unique() if d < today)
        prev_vols  = []
        for d in prev_dates:
            d_bars = df.loc[dates_series == d]
            if len(d_bars) == 0:
                continue
            window = d_bars.iloc[:n_today]["volume"].sum()
            if window > 0:
                prev_vols.append(float(window))

        if prev_vols:
            avg_same_window = sum(prev_vols) / len(prev_vols)
            return round(today_vol / avg_same_window, 2) if avg_same_window > 0 else 0.0

        # Fallback: time-fraction using yfinance avg if available
        if avg_daily_vol > 0:
            market_open   = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
            elapsed_mins  = max(1.0, (now_et - market_open).total_seconds() / 60.0)
            time_fraction = min(1.0, elapsed_mins / 390.0)
            expected      = avg_daily_vol * time_fraction
            return round(today_vol / expected, 2) if expected > 0 else 0.0

        return 0.0
    except Exception:
        return 0.0
