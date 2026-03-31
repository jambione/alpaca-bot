# Alpaca Momentum Trading Bot

## Files
| File | Description |
|------|-------------|
| `alpaca_stocks_bot.py` | Core trading bot — runs the momentum strategy loop |
| `alpaca_dashboard.py` | Web dashboard — start/stop bot, view positions & trades live |
| `trade_log.csv` | Auto-generated trade history (created on first run) |
| `alpaca_bot.log` | Auto-generated bot log (created on first run) |

## Setup
```bash
pip install fastapi uvicorn alpaca-py pandas numpy
```

## Running
**Always run from this folder:**
```bash
cd alpaca_trading_bot
python alpaca_dashboard.py
```
Then open **http://localhost:8888** in your browser.

The dashboard lets you:
- Toggle paper / live trading
- Start and stop the bot
- View open positions and close them manually
- See today's trade count and P&L
- Browse trending StockTwits picks under $5 (refreshes every 5 min)
- Stream live bot logs

## Paper vs Live
The config panel on the dashboard has a **Paper Trading** toggle.
Default is **paper = ON**. Switch to live only when ready.

## API Keys
Keys are pre-configured. You can also set environment variables:
```bash
export ALPACA_API_KEY=your_key
export ALPACA_SECRET_KEY=your_secret
```
# alpaca-bot
# alpaca-bot
