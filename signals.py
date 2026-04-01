"""
signals.py — Multi-Indicator Strategy with CM RSI-2, OBV Oscillator, and %R Trend Exhaustion
"""

import numpy as np
import pandas as pd


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def ema(series: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average"""
    return series.ewm(span=period, adjust=False).mean()


def sma(series: pd.Series, period: int) -> pd.Series:
    """Simple Moving Average"""
    return series.rolling(period).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Standard RSI calculation"""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def williams_pr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    """Williams %R calculation"""
    hh = high.rolling(length).max()
    ll = low.rolling(length).min()
    return -100 * (hh - close) / (hh - ll).replace(0, np.nan)


def vwap_calc(df: pd.DataFrame) -> pd.Series:
    """Volume Weighted Average Price"""
    typical = (df["high"] + df["low"] + df["close"]) / 3
    if hasattr(df.index, "date"):
        day = pd.Series(df.index.date, index=df.index)
    else:
        day = pd.Series([d.date() for d in df.index], index=df.index)
    cum_tp = (typical * df["volume"]).groupby(day).cumsum()
    cum_vol = df["volume"].groupby(day).cumsum()
    return cum_tp / cum_vol.replace(0, np.nan)


def calc_rvol(df: pd.DataFrame, avg_daily_vol: int = 0) -> pd.Series:
    """Relative Volume calculation"""
    if len(df) < 20:
        return pd.Series(1.0, index=df.index)
    avg_vol = df["volume"].rolling(20).mean()
    return df["volume"] / avg_vol.replace(0, np.nan)


# ============================================================================
# CM RSI 2 (Larry Connors RSI-2 Strategy - Lower)
# Based on Pine Script by ChrisMoody
# ============================================================================

def compute_cm_rsi_lower(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    CM RSI-2 Logic:
    - RSI with 2-period RMA smoothing
    - Green (bullish) when: close > SMA(200) AND close < SMA(5) AND RSI < 10
    - This version checks if RSI is approaching the oversold threshold (< 25)
    """
    df = df.copy()
    
    # RSI-2 calculation (same as Pine script)
    delta = df["close"].diff()
    up = delta.clip(lower=0).ewm(alpha=0.5, adjust=False).mean()  # RMA with alpha=0.5 = 2-period
    down = (-delta.clip(upper=0)).ewm(alpha=0.5, adjust=False).mean()
    rsi2 = np.where(down == 0, 100, np.where(up == 0, 0, 100 - (100 / (1 + up / down))))
    df["cm_rsi"] = pd.Series(rsi2, index=df.index)
    
    # Moving averages for conditions
    df["ma_200"] = sma(df["close"], 200)
    df["ma_5"] = sma(df["close"], 5)
    
    # Check if approaching condition (RSI < 25 and price conditions met)
    df["cm_rsi_approaching"] = (
        (df["close"] > df["ma_200"]) &  # Price above 200 MA
        (df["close"] < df["ma_5"]) &     # Price below 5 MA (pullback)
        (df["cm_rsi"] < 25)              # RSI approaching oversold
    )
    
    # Full bullish signal (from original Pine - RSI < 10)
    df["cm_rsi_bullish"] = (
        (df["close"] > df["ma_200"]) &
        (df["close"] < df["ma_5"]) &
        (df["cm_rsi"] < 10)
    )
    
    return df


# ============================================================================
# ON BALANCE VOLUME OSCILLATOR
# Based on Pine Script by LazyBear
# ============================================================================

def compute_obv_oscillator(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    OBV Oscillator Logic:
    - OBV = Cumulative volume (positive when close up, negative when close down)
    - Oscillator = OBV - EMA(OBV, length)
    - Bullish when: oscillator > 0 AND oscillator rising (trending up)
    """
    df = df.copy()
    
    length = cfg.get("obv_length", 20)
    
    # Calculate OBV
    change = df["close"].diff()
    obv = pd.Series(0.0, index=df.index)
    obv[change > 0] = df["volume"][change > 0]
    obv[change < 0] = -df["volume"][change < 0]
    obv = obv.cumsum()
    df["obv"] = obv
    
    # Calculate EMA of OBV
    df["obv_ema"] = ema(obv, length)
    
    # Calculate oscillator
    df["obv_osc"] = df["obv"] - df["obv_ema"]
    
    # Check if oscillator is above zero and trending up
    df["obv_above_zero"] = df["obv_osc"] > 0
    df["obv_rising"] = df["obv_osc"] > df["obv_osc"].shift(1)
    df["obv_trending_up"] = df["obv_above_zero"] & df["obv_rising"]
    
    return df


# ============================================================================
# %R TREND EXHAUSTION
# Based on Pine Script by upslidedown
# ============================================================================

def compute_percent_r_exhaustion(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    %R Trend Exhaustion Logic:
    - Uses dual %R: Fast (21) and Slow (112)
    - Overbought zone: %R >= -threshold
    - Oversold zone: %R <= -100 + threshold
    
    ON DECK (Bull Trend Start):
    - Entering overbought zone AND have been in overbought for > 1 bar
    - This means: overbought NOW AND overbought 1 bar ago
    
    OFF DECK (Bull Trend Break):
    - Was overbought, now NOT overbought (exiting the zone)
    - This is the reversal signal
    """
    df = df.copy()
    
    threshold = cfg.get("rte_threshold", 20)
    
    # Calculate %R for fast (21) and slow (112) periods
    s_pr = williams_pr(df["high"], df["low"], df["close"], 21)  # Fast
    l_pr = williams_pr(df["high"], df["low"], df["close"], 112)  # Slow
    
    # Apply smoothing (matching Pine script)
    s_percentR = s_pr.ewm(span=7, adjust=False).mean()  # Fast smoothing
    l_percentR = l_pr.ewm(span=3, adjust=False).mean()  # Slow smoothing
    
    df["s_percentR"] = s_percentR
    df["l_percentR"] = l_percentR
    
    # Overbought/Oversold conditions
    # Overbought: BOTH fast AND slow %R are in the overbought zone
    df["overbought"] = (s_percentR >= -threshold) & (l_percentR >= -threshold)
    # Oversold: BOTH fast AND slow %R are in the oversold zone
    df["oversold"] = (s_percentR <= -100 + threshold) & (l_percentR <= -100 + threshold)
    
    # Previous bar states
    df["overbought_prev"] = df["overbought"].shift(1).fillna(False)
    df["oversold_prev"] = df["oversold"].shift(1).fillna(False)
    
    # ON DECK = Bull Trend Start
    # Condition: Currently overbought AND was overbought 1 bar ago (consecutive)
    # This means the trend has been established (more than just a brief spike)
    df["ob_trend_start"] = df["overbought"] & df["overbought_prev"]
    
    # OFF DECK = Bull Trend Break (Reversal)
    # Condition: Was overbought but no longer is (exiting the zone)
    df["ob_reversal"] = (~df["overbought"]) & df["overbought_prev"]
    
    # Same for bear side
    df["os_trend_start"] = df["oversold"] & df["oversold_prev"]
    df["os_reversal"] = (~df["oversold"]) & df["oversold_prev"]
    
    # Combined extreme condition for backward compatibility
    side = cfg.get("rte_side", "red").lower()
    if side == "red":
        df["rte_extreme"] = df["overbought"]
    else:
        df["rte_extreme"] = df["oversold"]
    
    # Reversal flag for backward compatibility
    df["rte_reversal"] = df["ob_reversal"] | df["os_reversal"]
    
    # Track consecutive bars in zone
    df["ob_consecutive"] = df["overbought"].cumsum() - df["overbought"].cumsum().where(~df["overbought"]).ffill().fillna(0)
    df["os_consecutive"] = df["oversold"].cumsum() - df["oversold"].cumsum().where(~df["oversold"]).ffill().fillna(0)
    
    # On deck with minimum consecutive bars
    min_bars = cfg.get("rte_min_bars", 2)  # Need at least 2 bars in zone
    df["ob_on_deck"] = df["ob_trend_start"] & (df["ob_consecutive"] >= min_bars)
    df["os_on_deck"] = df["os_trend_start"] & (df["os_consecutive"] >= min_bars)
    
    # Final average %R for display
    df["rte_final"] = ((s_percentR + l_percentR) / 2).round(2)
    
    return df


# ============================================================================
# OTHER INDICATORS (kept for compatibility)
# ============================================================================

def compute_rmi(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Relative Momentum Index (placeholder)"""
    df = df.copy()
    df["rmi_signal"] = pd.Series(False, index=df.index)
    return df


def compute_volume_trending_up(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Volume trending up (placeholder - OBV oscillator is more accurate)"""
    df = df.copy()
    df["volume_trending_up"] = pd.Series(False, index=df.index)
    return df


def compute_macd(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """MACD calculation for additional confirmation"""
    df = df.copy()
    fast = cfg.get("macd_fast", 12)
    slow = cfg.get("macd_slow", 26)
    sig = cfg.get("macd_signal", 9)
    
    ema_fast = ema(df["close"], fast)
    ema_slow = ema(df["close"], slow)
    
    df["macd_line"] = ema_fast - ema_slow
    df["macd_signal_line"] = ema(df["macd_line"], sig)
    df["macd_hist"] = df["macd_line"] - df["macd_signal_line"]
    
    # Bull/bear crossover
    df["macd_bull"] = (
        (df["macd_line"] > df["macd_signal_line"]) &
        (df["macd_line"].shift(1) <= df["macd_signal_line"].shift(1))
    )
    df["macd_bear"] = (
        (df["macd_line"] < df["macd_signal_line"]) &
        (df["macd_line"].shift(1) >= df["macd_signal_line"].shift(1))
    )
    
    return df


# ============================================================================
# MAIN SIGNAL COMPUTATION
# ============================================================================

def compute_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    Main signal computation combining all three indicators:
    1. CM RSI-2: approaching < 25 (price above 200 MA, below 5 MA)
    2. OBV Oscillator: above zero AND rising
    3. %R Trend Exhaustion: 
       - ON DECK = bull trend start (consecutive overbought bars)
       - OFF DECK = bull trend break (reversal from overbought)
    """
    df = df.copy()
    
    # Compute all indicators
    df = compute_cm_rsi_lower(df, cfg)
    df = compute_obv_oscillator(df, cfg)
    df = compute_percent_r_exhaustion(df, cfg)
    
    # Volume surge (relaxed)
    df["volume_surge"] = df["volume"] > (df["volume"].rolling(20).mean() * cfg.get("volume_surge_mult", 1.3))
    
    # Individual indicator signals
    df["signal_cm_rsi"] = df["cm_rsi_approaching"]
    df["signal_obv"] = df["obv_trending_up"]
    df["signal_on_deck"] = df["ob_on_deck"]  # ON DECK: Bull trend start
    df["signal_off_deck"] = df["ob_reversal"]  # OFF DECK: Bull trend break
    
    # BUY signal: All three indicators agree
    df["signal"] = "HOLD"
    buy_condition = (
        df["signal_cm_rsi"] &      # CM RSI approaching < 25
        df["signal_obv"] &         # OBV oscillator above zero and rising
        df["signal_on_deck"]        # ON DECK: Bull trend established
    )
    df.loc[buy_condition, "signal"] = "BUY"
    
    # Additional: OFF DECK signal (bull trend break = potential entry on pullback)
    df["signal_off_deck_buy"] = False
    off_deck_buy = (
        df["signal_cm_rsi"] &
        df["signal_obv"] &
        df["signal_off_deck"]  # OFF DECK: Bull trend break (reversal)
    )
    df.loc[off_deck_buy, "signal_off_deck_buy"] = True
    
    # Williams %R for additional context
    df["wr"] = williams_pr(df["high"], df["low"], df["close"], cfg.get("wr_length", 14))
    
    # Add aliases for dashboard compatibility
    df["rte_fast"] = df["s_percentR"]  # Fast %R line (smoothed 21-period)
    df["rte_slow"] = df["l_percentR"]  # Slow %R line (smoothed 112-period)
    df["rsi2"] = df["cm_rsi"]         # CM RSI-2 value
    df["rmi"] = df["cm_rsi"]          # Also alias as rmi for config-based threshold check
    df["rmi_signal"] = df["cm_rsi_approaching"]  # RSI-2 pass signal
    
    # Legacy names for backward compatibility with other parts of the system
    df["vol_trend_up"] = df["obv_trending_up"]   # OBV oscillator trending up
    df["volume_trend_up"] = df["obv_trending_up"]
    
    # Add box tracking for streak display
    df["rte_boxes_streak"] = df["ob_consecutive"].fillna(0).astype(int)
    df["rte_boxes_completed"] = df["ob_consecutive"].fillna(0).astype(int)
    
    # Add RSI column for backward compatibility (some code uses "rsi" instead of "rsi2" or "cm_rsi")
    df["rsi"] = df["cm_rsi"]
    
    # Add EMA columns for dashboard compatibility
    df["ema_short"] = ema(df["close"], cfg.get("ema_short", 8))
    df["ema_long"] = ema(df["close"], cfg.get("ema_long", 21))
    
    return df
