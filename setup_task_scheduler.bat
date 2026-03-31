@echo off
REM ============================================================
REM  setup_task_scheduler.bat
REM  Creates a Windows Scheduled Task to launch the Alpaca bot
REM  every weekday at 8:55 AM (gives it time to be ready by 9:15)
REM  Run this file ONCE as Administrator.
REM ============================================================

title Setup Alpaca Bot - Task Scheduler
echo.
echo ====================================================
echo  Alpaca Trading Bot - Task Scheduler Setup
echo ====================================================
echo.
echo This will create a scheduled task called "AlpacaTradingBot"
echo that launches start_bot.bat every weekday at 8:55 AM.
echo.
echo IMPORTANT: Run this script as Administrator.
echo.
pause

REM Get the directory where this script lives
set "BOT_DIR=%~dp0"
set "BAT_PATH=%BOT_DIR%start_bot.bat"

REM Check the bat file exists
if not exist "%BAT_PATH%" (
    echo ERROR: start_bot.bat not found at %BAT_PATH%
    pause
    exit /b 1
)

REM Delete any existing task with the same name (ignore errors)
schtasks /delete /tn "AlpacaTradingBot" /f >nul 2>&1

REM Create the new task
REM  - Runs every weekday (MON,TUE,WED,THU,FRI)
REM  - At 8:55 AM
REM  - Starts minimized (so it doesn't take focus)
schtasks /create ^
    /tn "AlpacaTradingBot" ^
    /tr "cmd /c start /min \"\" \"%BAT_PATH%\"" ^
    /sc WEEKLY ^
    /d MON,TUE,WED,THU,FRI ^
    /st 08:55 ^
    /rl HIGHEST ^
    /f

if errorlevel 1 (
    echo.
    echo ERROR: Failed to create task. Make sure you are running as Administrator.
    pause
    exit /b 1
)

echo.
echo ====================================================
echo  SUCCESS!
echo ====================================================
echo  Task "AlpacaTradingBot" created.
echo  It will run every weekday at 8:55 AM.
echo  The bot dashboard opens at: http://localhost:8888
echo  The bot auto-starts trading at 9:15 AM ET.
echo.
echo  To remove the task later, run:
echo    schtasks /delete /tn "AlpacaTradingBot" /f
echo ====================================================
echo.
pause
