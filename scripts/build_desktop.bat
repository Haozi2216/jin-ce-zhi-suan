@echo off
:: Desktop build script (Windows) - PyInstaller
:: Usage: double-click this file
setlocal EnableDelayedExpansion

set "PROJECT_DIR=%~dp0.."
set "PYTHON=%~dp0..\.venv\Scripts\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"

echo [build] Checking Python...
"%PYTHON%" --version

echo [build] Installing dependencies...
"%PYTHON%" -m pip install pyinstaller -q

echo [build] Starting build (onedir mode, first run ~5-10 min)...
cd /d "%PROJECT_DIR%"

if exist "dist" (
    echo [build] Cleaning old artifacts...
    rmdir /s /q "dist" 2>nul
)

REM Run PyInstaller
"%PYTHON%" -m PyInstaller desktop.spec --clean

echo.
echo [build] DONE!
echo.
echo To run:
echo   dist\  (folder with Chinese name)
echo.
pause
