from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


def connect_alpaca(cfg: dict):
    from alpaca.trading.client import TradingClient
    from alpaca.data.historical import StockHistoricalDataClient

    paper = cfg.get("paper", True)
    trading = TradingClient(cfg["api_key"], cfg["secret_key"], paper=paper)
    data = StockHistoricalDataClient(cfg["api_key"], cfg["secret_key"])
    return trading, data


def _get_feed_arg(cfg: dict = None) -> dict:
    try:
        from alpaca.data.enums import DataFeed as _DF
        cfg = cfg or {}
        feed_name = cfg.get("data_feed", "IEX").upper()
        feed = _DF.SIP if feed_name == "SIP" else _DF.IEX
        return {"feed": feed}
    except Exception:
        return {}


def fetch_bars(data_client, ticker: str, cfg: dict) -> Optional[pd.DataFrame]:
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
        tf = tf_map.get(cfg.get("bar_timeframe", "5Min"), TimeFrame(5, TimeFrameUnit.Minute))
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=tf,
            start=datetime.now(timezone.utc) - timedelta(days=10),
            limit=cfg.get("bar_count", 300),
            **_get_feed_arg(cfg),
        )
        bars = data_client.get_stock_bars(req).df
        if bars is None or bars.empty:
            return None
        if isinstance(bars.index, pd.MultiIndex):
            bars = bars.xs(ticker, level="symbol")
        bars = bars[["open", "high", "low", "close", "volume"]].copy()
        bars["close"] = pd.to_numeric(bars["close"], errors="coerce")
        return bars.dropna(subset=["close"])
    except Exception:
        return None


def fetch_bars_batch(data_client, tickers: list[str], cfg: dict) -> dict[str, pd.DataFrame]:
    if not tickers or data_client is None:
        return {}

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
        tf = tf_map.get(cfg.get("bar_timeframe", "5Min"), TimeFrame(5, TimeFrameUnit.Minute))
        req = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=tf,
            start=datetime.now(timezone.utc) - timedelta(days=10),
            limit=cfg.get("bar_count", 300),
            **_get_feed_arg(cfg),
        )
        bars = data_client.get_stock_bars(req).df
        if bars is None or bars.empty:
            return {}

        results: dict[str, pd.DataFrame] = {}
        if isinstance(bars.index, pd.MultiIndex):
            for ticker in tickers:
                try:
                    ticker_df = bars.xs(ticker, level="symbol")
                except KeyError:
                    continue
                ticker_df = ticker_df[["open", "high", "low", "close", "volume"]].copy()
                ticker_df["close"] = pd.to_numeric(ticker_df["close"], errors="coerce")
                ticker_df = ticker_df.dropna(subset=["close"])
                if not ticker_df.empty:
                    results[ticker] = ticker_df
        else:
            bars = bars[["open", "high", "low", "close", "volume"]].copy()
            bars["close"] = pd.to_numeric(bars["close"], errors="coerce")
            bars = bars.dropna(subset=["close"])
            if not bars.empty:
                results[tickers[0]] = bars
        return results
    except Exception:
        return {}


def get_latest_trade_price(data_client, ticker: str, cfg: dict = None) -> Optional[float]:
    cfg = cfg or {}
    try:
        from alpaca.data.requests import StockLatestBarRequest
        resp = data_client.get_stock_latest_bar(StockLatestBarRequest(
            symbol_or_symbols=ticker,
            **_get_feed_arg(cfg)
        ))
        bar = resp.get(ticker)
        return float(bar.close) if bar else None
    except Exception:
        return None


def get_latest_trade_prices(data_client, tickers: list[str], cfg: dict = None) -> dict[str, float]:
    cfg = cfg or {}
    prices: dict[str, float] = {}
    if not tickers or data_client is None:
        return prices
    try:
        from alpaca.data.requests import StockLatestBarRequest
        resp = data_client.get_stock_latest_bar(StockLatestBarRequest(
            symbol_or_symbols=tickers,
            **_get_feed_arg(cfg)
        ))
        for ticker in tickers:
            try:
                bar = resp.get(ticker)
                if bar is not None and getattr(bar, "close", None) is not None:
                    prices[ticker] = float(bar.close)
            except Exception:
                continue
    except Exception:
        pass
    return prices


def close_position(trading_client, ticker: str) -> bool:
    try:
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        positions = {p.symbol: int(float(p.qty)) for p in trading_client.get_all_positions()}
        qty = positions.get(ticker, 0)
        if qty < 1:
            return False
        trading_client.submit_order(MarketOrderRequest(
            symbol=ticker, qty=qty, side=OrderSide.SELL, time_in_force=TimeInForce.DAY
        ))
        return True
    except Exception:
        return False
