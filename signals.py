"""
signals.py — Simple Oversold Bounce Strategy (Recommended Base)
"""

import numpy as np
import pandas as pd

# Exported functions for compatibility
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

def vwap_calc(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    if hasattr(df.index, "date"):
        day = pd.Series(df.index.date, index=df.index)
    else:
        day = pd.Series([d.date() for d in df.index], index=df.index)
    cum_tp = (typical * df["volume"]).groupby(day).cumsum()
    cum_vol = df["volume"].groupby(day).cumsum()
    return cum_tp / cum_vol.replace(0, np.nan)

def williams_pr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    hh = high.rolling(length).max()
    ll = low.rolling(length).min()
    return -100 * (hh - close) / (hh - ll).replace(0, np.nan)

def calc_rvol(df: pd.DataFrame, avg_daily_vol: int = 0) -> pd.Series:
    if len(df) < 20:
        return pd.Series(1.0, index=df.index)
    avg_vol = df["volume"].rolling(20).mean()
    return df["volume"] / avg_vol.replace(0, np.nan)


# Dummy functions for compatibility
def compute_percent_r_exhaustion(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    df["rte_eligible"] = pd.Series(False, index=df.index)
    return df

def compute_rmi(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df["rmi_signal"] = pd.Series(False, index=df.index)
    return df

def compute_obv_oscillator(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df["obv_osc"] = pd.Series(0, index=df.index)
    return df

def compute_volume_trending_up(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df["volume_trending_up"] = pd.Series(False, index=df.index)
    return df

def compute_macd(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df["macd_bull"] = pd.Series(False, index=df.index)
    return df


def compute_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()

    # Simple oversold bounce with volume
    df["wr"] = williams_pr(df["high"], df["low"], df["close"], cfg.get("wr_length", 14))

    # Volume surge (relaxed)
    df["volume_surge"] = df["volume"] > (df["volume"].rolling(20).mean() * cfg.get("volume_surge_mult", 1.3))

    # BUY signal
    df["signal"] = "HOLD"
    buy_condition = (
        (df["wr"] < cfg.get("wr_oversold", -75)) & 
        df["volume_surge"]
    )
    df.loc[buy_condition, "signal"] = "BUY"

    return df