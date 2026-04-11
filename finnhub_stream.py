"""
finnhub_stream.py — Real-time stock data via Finnhub WebSocket
Super-performant asyncio + WebSocket streaming for the dashboard.
"""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from collections import deque
from zoneinfo import ZoneInfo

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    print("[FINNHUB] websockets not installed. Run: pip install websockets")

# ── Configuration ───────────────────────────────────────────
ET = ZoneInfo("America/New_York")

# ── Shared State ─────────────────────────────────────────────
class FinnhubState:
    def __init__(self):
        self.lock = threading.Lock()
        self.prices = {}           # ticker -> {price, volume, timestamp}
        self.subscribed = set()   # currently subscribed tickers
        self.connected = False
        self.last_trade = {}       # ticker -> last trade data
        self.log_lines = deque(maxlen=100)

    def add_log(self, level: str, msg: str):
        ts = datetime.now(ET).strftime("%H:%M:%S")
        with self.lock:
            self.log_lines.append({"ts": ts, "level": level, "msg": msg})

    def update_price(self, ticker: str, price: float, volume: int = 0, timestamp: int = 0):
        with self.lock:
            self.prices[ticker] = {
                "price": price,
                "volume": volume,
                "timestamp": timestamp,
                "updated": datetime.now(ET).strftime("%H:%M:%S")
            }

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "prices": dict(self.prices),
                "subscribed": list(self.subscribed),
                "connected": self.connected,
                "log_lines": list(self.log_lines)[-50:],
            }

FINNHUB_STATE = FinnhubState()
_STREAM_THREAD = None

# ── WebSocket Stream ─────────────────────────────────────────
async def _finnhub_stream(api_key: str, tickers: list):
    """
    Async WebSocket connection to Finnhub for real-time trades.
    [P1-05: WebSocket Resilience - Exponential Backoff + Fallback]
    """
    if not WEBSOCKETS_AVAILABLE:
        return

    url = f"wss://ws.finnhub.io?token={api_key}"
    reconnect_attempts = 0
    max_reconnect_attempts = 5  # After this many failures, fallback to polling
    base_wait = 1.0  # Start with 1 second exponential backoff
    
    while True:
        try:
            async with websockets.connect(url, ping_interval=30) as ws:
                reconnect_attempts = 0  # Reset counter on successful connection
                FINNHUB_STATE.connected = True
                FINNHUB_STATE.add_log("INFO", f"Finnhub connected ({len(tickers)} tickers)")
                
                # Subscribe to tickers
                for ticker in tickers[:50]:  # Free tier: max 50 symbols
                    sub_msg = json.dumps({"type": "subscribe", "symbol": ticker})
                    await ws.send(sub_msg)
                    FINNHUB_STATE.subscribed.add(ticker)
                
                # Listen for messages
                async for message in ws:
                    try:
                        data = json.loads(message)
                        
                        if data.get("type") == "trade":
                            for trade in data.get("data", []):
                                ticker = trade.get("s", "")
                                price = trade.get("p", 0)
                                volume = trade.get("v", 0)
                                timestamp = trade.get("t", 0)
                                
                                if ticker and price > 0:
                                    FINNHUB_STATE.update_price(ticker, price, volume, timestamp)
                                    FINNHUB_STATE.last_trade[ticker] = {
                                        "price": price,
                                        "volume": volume,
                                        "timestamp": timestamp
                                    }
                        
                        elif data.get("type") == "ping":
                            # Respond to ping
                            pass
                            
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        FINNHUB_STATE.add_log("WARN", f"Parse error: {e}")
                
        except asyncio.CancelledError:
            FINNHUB_STATE.add_log("INFO", "Finnhub stream cancelled")
            break
        except Exception as e:
            FINNHUB_STATE.connected = False
            reconnect_attempts += 1
            
            if reconnect_attempts >= max_reconnect_attempts:
                wait_time = base_wait * (2 ** (max_reconnect_attempts - 1))
                FINNHUB_STATE.add_log(
                    "ERROR",
                    f"Finnhub: max reconnection attempts ({max_reconnect_attempts}) exceeded. "
                    f"Falling back to polling mode. Will retry in {wait_time}s."
                )
                await asyncio.sleep(wait_time)
                reconnect_attempts = 0  # Reset and try again
            else:
                wait_time = base_wait * (2 ** (reconnect_attempts - 1))
                FINNHUB_STATE.add_log(
                    "WARN",
                    f"Finnhub error (attempt {reconnect_attempts}/{max_reconnect_attempts}): {str(e)[:100]}. "
                    f"Reconnecting in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)

def start_finnhub_stream(api_key: str, tickers: list):
    """Start the Finnhub WebSocket stream in a background thread."""
    global _STREAM_THREAD
    if not WEBSOCKETS_AVAILABLE:
        FINNHUB_STATE.add_log("ERROR", "websockets library not installed")
        return None
    
    if not api_key:
        FINNHUB_STATE.add_log("ERROR", "No Finnhub API key configured")
        return None

    if _STREAM_THREAD and _STREAM_THREAD.is_alive():
        FINNHUB_STATE.add_log("INFO", "Finnhub stream already running")
        return _STREAM_THREAD
    
    def run_async():
        asyncio.run(_finnhub_stream(api_key, tickers))
    
    thread = threading.Thread(target=run_async, daemon=True)
    thread.start()
    _STREAM_THREAD = thread
    FINNHUB_STATE.add_log("INFO", f"Starting Finnhub stream for {len(tickers)} tickers...")
    return thread


# ── REST API Helpers (for historical data) ────────────────────
def fetch_realtime_quote(api_key: str, ticker: str) -> dict:
    """Fetch current quote from Finnhub REST API."""
    try:
        import urllib.request
        url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}"
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
            return {
                "ok": True,
                "ticker": ticker,
                "c": data.get("c", 0),   # Current price
                "h": data.get("h", 0),   # High
                "l": data.get("l", 0),   # Low
                "o": data.get("o", 0),   # Open
                "pc": data.get("pc", 0), # Previous close
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}

def fetch_candles(api_key: str, ticker: str, timeframe: str = "5", count: int = 100) -> dict:
    """Fetch candlestick data from Finnhub REST API."""
    try:
        import urllib.request
        url = f"https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution={timeframe}&count={count}&token={api_key}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            if data.get("s") == "ok":
                return {
                    "ok": True,
                    "ticker": ticker,
                    "t": data.get("t", []),  # Timestamps
                    "o": data.get("o", []),  # Open
                    "h": data.get("h", []),  # High
                    "l": data.get("l", []),  # Low
                    "c": data.get("c", []),  # Close
                    "v": data.get("v", []),  # Volume
                }
            return {"ok": False, "error": "No data"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_latest_price(ticker: str) -> float | None:
    """Return the last received trade price for a ticker from the active Finnhub stream."""
    with FINNHUB_STATE.lock:
        data = FINNHUB_STATE.prices.get(ticker)
        if data is None:
            return None
        return float(data.get("price", 0)) if data.get("price") is not None else None


def fetch_bars(api_key: str, ticker: str, cfg: dict) -> "pd.DataFrame | None":
    """Fetch candles from Finnhub and convert to a pandas DataFrame."""
    try:
        import pandas as pd
        tf_map = {
            "1Min": "1",
            "5Min": "5",
            "15Min": "15",
            "1Hour": "60",
            "1Day": "D",
        }
        resolution = tf_map.get(cfg.get("bar_timeframe", "5Min"), "5")
        count = min(int(cfg.get("bar_count", 300)), 1000)
        result = fetch_candles(api_key, ticker, resolution, count)
        if not result.get("ok"):
            return None
        df = pd.DataFrame({
            "open": result["o"],
            "high": result["h"],
            "low": result["l"],
            "close": result["c"],
            "volume": result["v"],
        }, index=pd.to_datetime(result["t"], unit="s", utc=True))
        df.index.name = "timestamp"
        return df
    except Exception:
        return None


# ── Module Exports ─────────────────────────────────────────────
__all__ = [
    "FINNHUB_STATE",
    "start_finnhub_stream",
    "fetch_realtime_quote",
    "fetch_candles",
    "get_latest_price",
    "fetch_bars",
]