@echo off
title Alpaca Trading Bot
cd /d "%~dp0"

echo =======================================
echo  Alpaca Trading Bot - Starting Server
echo =======================================
echo.

REM Check Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found. Make sure Python is installed and on your PATH.
    pause
    exit /b 1
)

REM Install dependencies if needed
echo Checking dependencies...
python -m pip install fastapi uvicorn alpaca-py pandas numpy yfinance websockets --quiet

echo.
echo Starting dashboard on http://localhost:8888
echo The bot will auto-start at 9:15 AM ET on weekdays.
echo Close this window to stop the bot.
echo.

python alpaca_dashboard.py

pause
