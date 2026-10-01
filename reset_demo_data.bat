@echo off
setlocal
cd /d "%~dp0"

echo ========================================================
echo   Exam Resilience Control Tower (ERCT) - Reset Demo Data
echo ========================================================
echo.
echo This utility will delete demo SQLite database files:
echo   - data\erct.db*
echo   - data\simulator_buffer.db*
echo.
echo NOTE: Logs and data\runs will NOT be deleted.
echo.
set /p CONFIRM="Are you sure you want to reset demo data? (y/N): "
if /i "%CONFIRM:~0,1%" neq "y" (
    echo.
    echo Reset cancelled. No files were deleted.
    echo.
    pause
    exit /b 0
)

echo.
echo Deleting database files...
if exist "data\erct.db*" (
    del /f /q "data\erct.db*" 2>nul
    echo Deleted data\erct.db*
)
if exist "data\simulator_buffer.db*" (
    del /f /q "data\simulator_buffer.db*" 2>nul
    echo Deleted data\simulator_buffer.db*
)

echo.
echo Demo database files reset successfully. Ready for a fresh recording.
echo.
pause
