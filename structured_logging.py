"""
structured_logging.py — JSON-structured logging for operational visibility

[P1-06: Structured Logging Architecture]

Provides JSON-formatted logs that can be parsed, aggregated, and sent to
observability stacks (ELK, Datadog, CloudWatch, etc.)
"""

import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import os

ET = ZoneInfo("America/New_York")


class StructuredFormatter(logging.Formatter):
    """
    Formats log records as JSON with sanitized sensitive data.
    Automatically includes timestamp, level, logger name, and message.
    """
    
    SENSITIVE_KEYS = {
        'api_key', 'secret_key', 'password', 'token', 'secret',
        'key', 'credential', 'auth', 'apikey'
    }
    
    def sanitize(self, obj, depth=0):
        """Recursively sanitize sensitive data from objects."""
        if depth > 10:  # Prevent infinite recursion
            return str(obj)[:100]
        
        if isinstance(obj, dict):
            return {
                k: "***REDACTED***" if any(s in k.lower() for s in self.SENSITIVE_KEYS) else self.sanitize(v, depth + 1)
                for k, v in obj.items()
            }
        elif isinstance(obj, (list, tuple)):
            return [self.sanitize(v, depth + 1) for v in obj]
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')[:50]
        else:
            return obj
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_dict = {
            "timestamp": datetime.now(ET).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Include exception info if present
        if record.exc_info:
            log_dict["exception"] = self.format_exception(*record.exc_info)
        
        # Sanitize any extra fields added to the record
        if hasattr(record, '__dict__'):
            for key, value in record.__dict__.items():
                if key.startswith('_') or key in {'name', 'msg', 'args', 'created', 'filename', 'funcName', 'levelname', 'levelno', 'lineno', 'module', 'msecs', 'message', 'pathname', 'process', 'processName', 'relativeCreated', 'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info', 'taskName'}:
                    continue
                if not key.startswith('_'):
                    log_dict[key] = self.sanitize(value)
        
        return json.dumps(log_dict, default=str)
    
    @staticmethod
    def format_exception(exc_type, exc_value, exc_tb):
        """Format exception as dict."""
        import traceback
        return {
            "type": exc_type.__name__ if exc_type else "Unknown",
            "message": str(exc_value),
            "traceback": "".join(traceback.format_tb(exc_tb)).split("\n")[:10],  # First 10 lines
        }


def setup_structured_logging(log_file: str = "alpaca_bot.log", level=logging.INFO):
    """
    Configure structured JSON logging for the bot.
    
    Args:
        log_file: Path to log file
        level: Logging level (default INFO)
    """
    logger = logging.getLogger("alpaca_bot")
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler with structured formatter
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(StructuredFormatter())
    logger.addHandler(file_handler)
    
    # Optional: Console handler (also structured)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(StructuredFormatter())
    logger.addHandler(console_handler)
    
    logger.setLevel(level)
    
    return logger


# Convenience functions for common log patterns
def log_trade(logger: logging.Logger, ticker: str, action: str, qty: int, price: float, **kwargs):
    """Log a trade event."""
    logger.info("trade", extra={
        "ticker": ticker,
        "action": action,  # "buy" | "sell" | "close_position"
        "qty": qty,
        "price": price,
        "timestamp": datetime.now(ET).isoformat(),
        **kwargs
    })


def log_signal(logger: logging.Logger, ticker: str, signal: str, reasons: list, indicators: dict, **kwargs):
    """Log a strategy signal."""
    logger.info("signal", extra={
        "ticker": ticker,
        "signal": signal,  # "BUY" | "SELL" | "HOLD"
        "reasons": reasons,
        "indicators": indicators,
        **kwargs
    })


def log_api_error(logger: logging.Logger, endpoint: str, error: str, retry_attempt: int = None, **kwargs):
    """Log API errors with retry context."""
    logger.warning("api_error", extra={
        "endpoint": endpoint,
        "error": str(error)[:200],
        "retry_attempt": retry_attempt,
        **kwargs
    })


def log_state_mismatch(logger: logging.Logger, mismatch_type: str, details: dict, **kwargs):
    """Log state reconciliation mismatches."""
    logger.warning("state_mismatch", extra={
        "mismatch_type": mismatch_type,
        "details": details,
        **kwargs
    })
