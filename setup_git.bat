@echo off
echo ==========================================
echo  Alpaca Bot - Git Setup
echo ==========================================
echo.

cd /d "%~dp0"

REM ── Check git is installed ─────────────────
where git >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: git not found. Install from https://git-scm.com/download/win
    pause
    exit /b 1
)

REM ── Init repo ──────────────────────────────
git init
git branch -M main

REM ── Identity ───────────────────────────────
git config user.name  "jambione"
git config user.email "ngs2ts9kcf@privaterelay.appleid.com"

REM ── Stage files (bot_config.json excluded by .gitignore) ──
git add alpaca_dashboard.py
git add backtest.py
git add bot_config.example.json
git add .gitignore
git add README.md
git add check_signals.py
git add setup_task_scheduler.bat
git add start_bot.bat
git add signals.py
git add trending.py
git add dashboard.html

echo.
echo Files staged:
git status --short

REM ── Initial commit ─────────────────────────
git commit -m "Initial commit — working bot with Williams %%R exhaustion strategy

Features included:
- Williams %%R Exhaustion strategy (red side momentum breakout)
- Fast-scan On Deck watching set with live %%R bar updates
- On Deck entry/exit event feed with signal conditions
- Fast-watch open positions for stop/TP/trail exits
- ATR-based position sizing (use_atr_sizing)
- Multi-timeframe 1-min confirmation (use_mtf_confirm)
- Bracket orders — server-side stop+TP (use_bracket_orders)
- Live RVOL gate at entry
- Morning session filter (no_new_buys_before)
- Atomic config writes, PDT detection, T+2 cash sizing
- Full backtest engine with walk-forward support

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"

REM ── Remote + push ──────────────────────────
echo.
echo Adding remote origin...
git remote add origin https://github.com/jambione/alpaca-bot.git

echo.
echo Pushing to GitHub...
echo (A browser window or credential prompt may appear)
git push -u origin main

echo.
if %ERRORLEVEL% EQU 0 (
    echo SUCCESS! Code is now at https://github.com/jambione/alpaca-bot
) else (
    echo Push failed. You may need to authenticate.
    echo Try: git push -u origin main
    echo Or set up a Personal Access Token at https://github.com/settings/tokens
)

pause
