@echo off
REM SWORD QA Reviewer - One-click launch (Windows)
REM Run this script to start the reviewer UI.

echo Checking setup...
python scripts/maintenance/check_reviewer_setup.py
if errorlevel 1 (
    echo.
    echo Setup check failed. Fix the issues above and try again.
    pause
    exit /b 1
)

echo.
echo Launching SWORD Reviewer...
streamlit run deploy/reviewer/app.py
pause
