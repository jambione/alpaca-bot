from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import time
import logging

import pandas as pd

# ─── Rate-Limit Retry Logic [P1-02: API Rate-Limit Handling] ───────────────────
def retry_with_backoff(max_retries: int = 3, base_wait: float = 1.0):
    """
    Decorator for API calls to handle rate limits (429) and transient errors (503).
    Uses exponential backoff: wait = base_wait * (2 ^ attempt).
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Check if error is rate-limit or service unavailable
                    error_str = str(e)
                    is_rate_limit = "429" in error_str or "rate" in error_str.lower()
                    is_unavailable = "503" in error_str or "service unavailable" in error_str.lower()
                    
                    if (is_rate_limit or is_unavailable) and attempt < max_retries - 1:
                        wait_time = base_wait * (2 ** attempt)
                        logging.warning(
                            f"[RATE-LIMIT-RETRY] {func.__name__} attempt {attempt + 1}/{max_retries} — "
                            f"waiting {wait_time}s before retry. Error: {error_str[:100]}"
                        )
                        time.sleep(wait_time)
                    else:
                        # Non-rate-limit error or final attempt
                        raise
            return None
        return wrapper
    return decorator


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
    @retry_with_backoff(max_retries=3, base_wait=1.0)
    def _fetch():
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
    
    try:
        return _fetch()
    except Exception:
        return None


def fetch_bars_batch(data_client, tickers: list[str], cfg: dict) -> dict[str, pd.DataFrame]:
    if not tickers or data_client is None:
        return {}

    @retry_with_backoff(max_retries=3, base_wait=1.0)
    def _fetch():
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
    
    try:
        return _fetch()
    except Exception:
        return {}


def get_latest_trade_price(data_client, ticker: str, cfg: dict = None) -> Optional[float]:
    cfg = cfg or {}
    
    @retry_with_backoff(max_retries=3, base_wait=1.0)
    def _fetch():
        from alpaca.data.requests import StockLatestBarRequest
        resp = data_client.get_stock_latest_bar(StockLatestBarRequest(
            symbol_or_symbols=ticker,
            **_get_feed_arg(cfg)
        ))
        bar = resp.get(ticker)
        return float(bar.close) if bar else None
    
    try:
        return _fetch()
    except Exception:
        return None


def get_latest_trade_prices(data_client, tickers: list[str], cfg: dict = None) -> dict[str, float]:
    cfg = cfg or {}
    prices: dict[str, float] = {}
    if not tickers or data_client is None:
        return prices
    
    @retry_with_backoff(max_retries=3, base_wait=1.0)
    def _fetch():
        from alpaca.data.requests import StockLatestBarRequest
        resp = data_client.get_stock_latest_bar(StockLatestBarRequest(
            symbol_or_symbols=tickers,
            **_get_feed_arg(cfg)
        ))
        results: dict[str, float] = {}
        for ticker in tickers:
            try:
                bar = resp.get(ticker)
                if bar is not None and getattr(bar, "close", None) is not None:
                    results[ticker] = float(bar.close)
            except Exception:
                continue
        return results
    
    try:
        return _fetch()
    except Exception:
        return {}


def close_position(trading_client, ticker: str) -> bool:
    @retry_with_backoff(max_retries=3, base_wait=1.0)
    def _close():
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
    
    try:
        return _close()
    except Exception:
        return False


# ─── Crash Recovery Protocol [P1-04: State Reconciliation] ────────────────────
def reconcile_state_with_server(trading_client, local_state: dict) -> dict:
    """
    Reconcile local position state with Alpaca server state on bot startup.
    Prevents double-trades and state mismatches after crashes.
    
    Returns: {
        'status': 'ok' | 'mismatch' | 'error',
        'local_positions': [...],
        'server_positions': [...],
        'mismatches': [...],
        'recovery_actions': [...]
    }
    """
    result = {
        "status": "ok",
        "local_positions": [],
        "server_positions": [],
        "mismatches": [],
        "recovery_actions": [],
    }
    
    try:
        # Fetch actual positions from Alpaca
        server_positions = trading_client.get_all_positions()
        server_tickers = {p.symbol: {"qty": int(float(p.qty)), "entry_price": float(p.avg_fill_price)} for p in server_positions}
        result["server_positions"] = list(server_tickers.keys())
        result["local_positions"] = [p["ticker"] for p in local_state.get("positions", [])]
        
        # Compare local vs server
        local_tickers = {p["ticker"]: p for p in local_state.get("positions", [])}
        
        # Check for ghost positions (in server but not local)
        for server_ticker, server_data in server_tickers.items():
            if server_ticker not in local_tickers:
                result["mismatches"].append({
                    "type": "ghost_position",
                    "ticker": server_ticker,
                    "qty": server_data["qty"],
                    "entry_price": server_data["entry_price"],
                    "action": "Flag for manual review — position exists on server but not in local state",
                })
                result["status"] = "mismatch"
        
        # Check for orphaned positions (in local but not server)
        for local_ticker, local_data in local_tickers.items():
            if local_ticker not in server_tickers:
                result["mismatches"].append({
                    "type": "orphaned_position",
                    "ticker": local_ticker,
                    "local_qty": local_data.get("qty", 0),
                    "action": "Remove from local state — position was closed/liquidated",
                })
                result["status"] = "mismatch"
        
        # Check for qty mismatches
        for local_ticker, local_data in local_tickers.items():
            if local_ticker in server_tickers:
                local_qty = local_data.get("qty", 0)
                server_qty = server_tickers[local_ticker]["qty"]
                if local_qty != server_qty:
                    result["mismatches"].append({
                        "type": "qty_mismatch",
                        "ticker": local_ticker,
                        "local_qty": local_qty,
                        "server_qty": server_qty,
                        "action": f"Update local qty to {server_qty}",
                    })
                    result["status"] = "mismatch"
                    result["recovery_actions"].append({
                        "ticker": local_ticker,
                        "action": "update_qty",
                        "new_qty": server_qty,
                    })
        
        if result["status"] == "mismatch":
            logging.warning(
                f"[RECOVERY] State mismatch detected. Local: {result['local_positions']}, "
                f"Server: {result['server_positions']}. Mismatches: {len(result['mismatches'])}"
            )
        else:
            logging.info("[RECOVERY] Local state matches server state — no recovery needed")
        
        return result
        
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        logging.error(f"[RECOVERY] Reconciliation failed: {str(e)[:200]}")
        return result
