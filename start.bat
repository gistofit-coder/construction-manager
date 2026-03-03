@echo off
setlocal
title KBWeb - Construction Business Manager

REM ── Find Python ─────────────────────────────────────────────
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python not found in PATH.
    echo Please install Python 3.11+ from https://python.org/downloads
    echo Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)

REM ── Use the folder where start.bat lives as the app folder ───
cd /d "%~dp0"

REM ── Install/update dependencies silently ────────────────────
echo Installing dependencies...
python -m pip install -r requirements.txt -q --disable-pip-version-check
if %errorlevel% neq 0 (
    echo WARNING: Could not install some packages. App may not work correctly.
)

REM ── Set database to local construction.db ───────────────────
set CONSTRUCTION_DB=%~dp0construction.db

REM ── Open browser after 3 seconds ────────────────────────────
start "" cmd /c "timeout /t 3 >nul && start http://localhost:5000"

REM ── Start the app ────────────────────────────────────────────
echo.
echo ============================================================
echo   KBWeb Construction Business Manager
echo   Open: http://localhost:5000
echo   Press Ctrl+C to stop
echo ============================================================
echo.
python app.py
if %errorlevel% neq 0 (
    echo.
    echo ERROR: App exited with an error. See message above.
    pause
)
