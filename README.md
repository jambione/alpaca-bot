# Alpaca Momentum Trading Bot

## Files

| File                   | Description                                                  |
| ---------------------- | ------------------------------------------------------------ | --- | -------------------------- | ---------------------------------------------------- | --- | --------------- | --------------------------------------------------- |
| `alpaca_stocks_bot.py` | Core trading bot — runs the momentum strategy loop           |
| `alpaca_dashboard.py`  | Web dashboard — start/stop bot, view positions & trades live |     | `screen_ticker_scanner.py` | Local live OCR scanner for video/screen ticker feeds |     | `trade_log.csv` | Auto-generated trade history (created on first run) |
| `alpaca_bot.log`       | Auto-generated bot log (created on first run)                |

## Setup

```bash
pip install fastapi uvicorn alpaca-py pandas numpy websockets
```

To use the local screen OCR ticker scanner, install these additional open-source packages:

```bash
pip install mss opencv-python easyocr pyyaml
```

You can also install everything via:

```bash
pip install -r requirements.txt
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

## Finnhub realtime feed

The dashboard now supports Finnhub as the realtime quote source using `finnhub_key` from `bot_config.json` or `FINNHUB_API_KEY`.

## Finnhub quick start

```python
import asyncio
import json
import websockets
import os

API_KEY = os.getenv("FINNHUB_API_KEY")  # Get free key at finnhub.io

async def stock_stream(tickers: list[str]):
    url = f"wss://ws.finnhub.io?token={API_KEY}"
    async with websockets.connect(url) as ws:
        for ticker in tickers:
            await ws.send(json.dumps({"type": "subscribe", "symbol": ticker}))

        async for message in ws:
            data = json.loads(message)
            if data.get("type") == "trade":
                for trade in data.get("data", []):
                    print(f"{trade.get('s')} @ {trade.get('p')} (vol: {trade.get('v')})")

asyncio.run(stock_stream(["AAPL", "TSLA", "NVDA"]))
```

## Paper vs Live

The config panel on the dashboard has a **Paper Trading** toggle.
Default is **paper = ON**. Switch to live only when ready.

## API Keys

Keys are pre-configured. You can also set environment variables:

```bash
export ALPACA_API_KEY=your_key
export ALPACA_SECRET_KEY=your_secret
export FINNHUB_API_KEY=your_finnhub_key
```

# alpaca-bot

# alpaca-bot
