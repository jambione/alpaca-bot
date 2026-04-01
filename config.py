import json
import os
import tempfile
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / "bot_config.json"

DEFAULT_CONFIG = {
    "api_key": os.getenv("ALPACA_API_KEY", ""),
    "secret_key": os.getenv("ALPACA_SECRET_KEY", ""),
    "finnhub_key": os.getenv("FINNHUB_API_KEY", ""),
    "paper": True,
    "tickers": [],
    "scan_region_top": 200,
    "scan_region_left": 300,
    "scan_region_width": 1200,
    "scan_region_height": 600,
    "ema_short": 8,
    "ema_long": 21,
    "rsi_period": 14,
    "rsi_min_buy": 45,
    "rsi_overbought": 65,
    "rsi_sell": 75,
    "volume_surge_mult": 1.5,
    "use_vwap": True,
    "position_size_pct": 0.10,
    "max_positions": 10,
    "max_daily_buys": 10,
    "stop_loss_pct": 0.04,
    "take_profit_pct": 0.12,
    "trail_activation_pct": 0.05,
    "trail_stop_pct": 0.02,
    "pyramid_enabled": True,
    "pyramid_gain_pct": 0.05,
    "pyramid_rsi_max": 68,
    "pyramid_size_pct": 0.10,
    "no_new_buys_before": [10, 0],
    "no_new_buys_after": [15, 30],
    "eod_liquidate_at": [16, 0],
    "scan_interval_sec": 60,
    "bar_timeframe": "1Min",
    "bar_count": 800,
    "use_float_filter": True,
    "max_float_million": 50,
    "micro_float_threshold": 10,
    "use_rvol": True,
    "min_rvol": 2.0,
    "partial_exit_enabled": True,
    "partial_exit_pct": 0.06,
    "partial_exit_qty_pct": 0.50,
    "auto_schedule": True,
    "market_open_at": [9, 15],
    "trending_max_price": 5.0,
    "wr_length": 14,
    "wr_oversold": -80,
    "wr_overbought": -20,
    "cm_rsi_length": 14,
    "cm_rsi_oversold": 30,
    "cm_rsi_overbought": 70,
    "obv_length": 20,
    "vol_trend_short": 10,
    "vol_trend_long": 50,
    "strategy": "exhaustion",
    "use_rte_exhaustion": True,
    "use_rmi": True,
    "use_volume_trending_up": True,
    "use_macd": True,
    "rte_side": "red",
    "rte_threshold": 20,
    "rte_avg_ma": 3,
    "rte_min_boxes": 2,
    "rte_entry_window": 15,
    "rte_min_supporting": 1,
    "rmi_oversold": 10,
    "rmi_ma_fast": 20,
    "rmi_ma_slow": 200,
    "precheck_threshold": -40,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "pre_market_enabled": False,
    "pre_market_start": [4, 0],
    "pre_market_limit_offset_pct": 0.002,
    "pre_market_volume_surge_mult": 0.3,
    "use_atr_sizing": False,
    "atr_period": 14,
    "risk_per_trade_pct": 0.02,
    "atr_risk_mult": 1.5,
    "use_mtf_confirm": False,
    "mtf_timeframe": "1Min",
    "mtf_bar_count": 60,
    "use_bracket_orders": False,
    "max_daily_loss_pct": 0.05,
    "debug_signals": True,
    "tv_chart_url": "https://www.tradingview.com/chart/x04Gfcu8/",
    "finviz_enabled": True,
    "finviz_exchange": "NASDAQ",
    "finviz_performance": "Week Up",
    "finviz_rel_volume": "Over 2",
    "finviz_avg_volume": "Over 500K",
    "finviz_float": "Under 20M",
    "finviz_signal": "",
    "finviz_include_news": True,
    "signal_sell_enabled": False,
    "momentum_quality_rsi_min": 25,
    "momentum_quality_rsi_max": 85,
    "rte_max_entry_extension_pct": 0.05,
}

SAFE_CONFIG_KEYS = [
    "paper","api_key","secret_key","tickers",
    "scan_region_top","scan_region_left","scan_region_width","scan_region_height",
    "ema_short","ema_long","rsi_period","rsi_min_buy","rsi_overbought","rsi_sell",
    "volume_surge_mult","use_vwap",
    "position_size_pct","max_positions","max_daily_buys",
    "stop_loss_pct","take_profit_pct",
    "trail_activation_pct","trail_stop_pct",
    "pyramid_enabled","pyramid_gain_pct","pyramid_rsi_max","pyramid_size_pct",
    "use_float_filter","max_float_million","use_rvol","min_rvol",
    "trending_max_price",
    "partial_exit_enabled","partial_exit_pct","partial_exit_qty_pct",
    "auto_schedule","market_open_at",
    "no_new_buys_before","no_new_buys_after","eod_liquidate_at","scan_interval_sec","bar_timeframe",
    "strategy","use_rte_exhaustion","use_rmi","use_volume_trending_up","use_macd",
    "rte_side","rte_threshold","rte_avg_ma","rte_min_boxes",
    "rmi_oversold","rmi_ma_fast","rmi_ma_slow","precheck_threshold",
    "macd_fast","macd_slow","macd_signal",
    "tv_chart_url","debug_signals","pre_market_enabled",
    "pre_market_start","pre_market_limit_offset_pct","pre_market_volume_surge_mult",
    "max_daily_loss_pct","bar_count","use_atr_sizing","atr_period",
    "risk_per_trade_pct","atr_risk_mult","use_mtf_confirm","mtf_timeframe",
    "mtf_bar_count","use_bracket_orders","signal_sell_enabled",
    "rte_entry_window","rte_min_supporting","micro_float_threshold",
    "momentum_quality_rsi_min","momentum_quality_rsi_max","rte_max_entry_extension_pct",
    "finviz_enabled","finviz_exchange","finviz_performance","finviz_rel_volume",
    "finviz_avg_volume","finviz_float","finviz_signal","finviz_include_news",
]


def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                cfg.update(saved)
            print(f"[CFG] Loaded config from {CONFIG_FILE}")
        except Exception as e:
            print(f"[CFG] Failed to load config ({e}) — using defaults")
    else:
        print("[CFG] No saved config found — using defaults")
    return cfg


def save_config(cfg: dict):
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(dir=CONFIG_FILE.parent, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            Path(tmp_path).replace(CONFIG_FILE)
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            raise
    except Exception as e:
        print(f"[CFG] Failed to save config: {e}")
