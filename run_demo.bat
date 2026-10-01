@echo off
setlocal

echo ========================================================
echo   Exam Resilience Control Tower (ERCT) - Demo Launcher
echo ========================================================
echo.

cd /d "%~dp0"

echo [1/4] Starting API server in separate console window...
start "ERCT API Server" python -m uvicorn app.main:app --host 127.0.0.1 --port 8000

echo [2/4] Waiting for GET /v1/health to answer (polling up to 40 s)...
set /a REMAINING=40

:poll_api
python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/v1/health', timeout=2).getcode() == 200 else 1)" 2>nul
if %ERRORLEVEL% equ 0 (
    echo [OK] API server is live and healthy.
    goto api_ready
)

set /a REMAINING-=1
if %REMAINING% leq 0 (
    echo [ERROR] Timed out waiting for API /v1/health after 40 seconds.
    echo Please check the ERCT API Server console window for error details.
    pause
    exit /b 1
)

ping -n 2 127.0.0.1 >nul
goto poll_api

:api_ready
echo.
echo [3/4] Starting Multi-Agent Simulator in separate console window...
start "ERCT Multi-Agent Simulator" python -m simulator.agent_runner

echo.
echo [4/4] Waiting 20 seconds for detection start-up grace period...
for /l %%i in (20,-1,1) do (
    echo    Detection grace: %%i seconds remaining...
    ping -n 2 127.0.0.1 >nul
)
echo    Start-up grace complete.
echo.

echo Launching Control Tower GUI (python gui.py)...
python gui.py %*

echo.
echo ========================================================
echo   Control Tower GUI session closed.
echo.
echo   REMINDER: The ERCT API Server and Multi-Agent Simulator
echo   console windows are still running in the background.
echo   Please close both console windows when finished.
echo ========================================================
echo.
pause
