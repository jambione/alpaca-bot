import csv
from datetime import datetime, timedelta
from pathlib import Path

TRADE_LOG_FILE = Path("trade_log.csv")
TRADE_COLUMNS = [
    "date", "time", "ticker", "action", "qty", "price", "position_value",
    "entry_price", "pnl_dollars", "pnl_pct", "reason",
    "streak", "float_m", "rvol", "rsi2", "rte_fast", "rte_slow", "conviction",
]


def init_trade_log():
    if not TRADE_LOG_FILE.exists():
        with open(TRADE_LOG_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(["date", "time", "ticker", "action", "qty", "price", "position_value",
                                     "entry_price", "pnl_dollars", "pnl_pct", "reason"])


def log_trade(ticker: str, action: str, qty: int, price: float, entry_price: float = None, reason: str = "SIGNAL"):
    now = datetime.now()
    pnl_dollars = pnl_pct = ""
    if entry_price and action in ("SELL", "STOP", "TP", "EOD", "TRAIL", "PARTIAL", "EXHAUSTION"):
        pnl_dollars = round((price - entry_price) * qty, 2)
        pnl_pct = round(((price - entry_price) / entry_price) * 100, 2)

    with open(TRADE_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S"), ticker, action, qty,
            round(price, 2), round(price * qty, 2),
            round(entry_price, 2) if entry_price else "",
            pnl_dollars, pnl_pct, reason
        ])


def load_today_trades() -> list:
    today = datetime.now().strftime("%Y-%m-%d")
    rows = []
    if not TRADE_LOG_FILE.exists():
        return rows
    try:
        with open(TRADE_LOG_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("date") == today:
                    rows.append(row)
    except Exception:
        pass
    return rows


def load_all_trades(days: int = 90) -> list:
    rows = []
    if not TRADE_LOG_FILE.exists():
        return rows
    cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
    try:
        with open(TRADE_LOG_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("date", "") >= cutoff:
                    rows.append(row)
    except Exception:
        pass
    return rows
