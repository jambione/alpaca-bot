import math
import threading
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo

from config import DEFAULT_CONFIG

ET = ZoneInfo("America/New_York")


def _sanitize_floats(obj):
    """Recursively replace float NaN / Inf with None so json.dumps produces valid JSON."""
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_floats(v) for v in obj]
    return obj


class BotState:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.stop_event = threading.Event()
        self.config = DEFAULT_CONFIG.copy()
        self.account = {}
        self.positions = []
        self.entry_prices = {}
        self.high_water = {}
        self.pyramided = set()
        self.daily_buys = 0
        self.last_scan = None
        self.log_lines = deque(maxlen=300)
        self.trading_client = None
        self.data_client = None
        self.error = None
        self.today_trades = []
        self.trending_tickers = []
        self.trending_sources = {}
        self.trending_prices = {}
        self.trending_updated = None
        self.manually_closed = set()
        self.watching = {}
        self.ondeck_events = deque(maxlen=30)
        self.partial_exited = set()
        self.fast_closing = set()

    def add_log(self, level: str, msg: str):
        ts = datetime.now(ET).strftime("%H:%M:%S")
        with self.lock:
            self.log_lines.append({"ts": ts, "level": level, "msg": msg})

    def add_ondeck_event(self, direction: str, ticker: str, reason: str, conditions: dict = None):
        """direction: 'enter' | 'exit_buy' | 'exit_fail' | 'exit_manual' | 'exit_position'"""
        ts = datetime.now(ET).strftime("%H:%M:%S")
        with self.lock:
            self.ondeck_events.append({
                "ts": ts,
                "direction": direction,
                "ticker": ticker,
                "reason": reason,
                "conditions": conditions or {},
            })

    def snapshot(self, include_static: bool = False) -> dict:
        """
        Return the current state.
        include_static=True  → full payload including config + trades (sent once on connect + on change)
        include_static=False → lightweight payload for 3-second ticks (positions, logs, account only)
        """
        with self.lock:
            data = {
                "running": self.running,
                "paper": self.config.get("paper", True),
                "account": dict(self.account),
                "positions": list(self.positions),
                "entry_prices": dict(self.entry_prices),
                "high_water": dict(self.high_water),
                "daily_buys": self.daily_buys,
                "max_daily_buys": self.config.get("max_daily_buys", 10),
                "last_scan": self.last_scan,
                "log_lines": list(self.log_lines)[-300:],
                "error": self.error,
                "manually_closed": sorted(self.manually_closed),
                "watching": _sanitize_floats({t: dict(v) for t, v in self.watching.items()}),
                "ondeck_events": _sanitize_floats(list(self.ondeck_events)[-10:]),
            }
            if include_static:
                data["config"] = dict(self.config)
                data["today_trades"] = list(self.today_trades)
                data["trending_tickers"] = list(self.trending_tickers)
                data["trending_sources"] = dict(self.trending_sources)
                data["trending_prices"] = dict(self.trending_prices)
                data["trending_updated"] = self.trending_updated
            return data
