@echo off
REM Script to set up and run nptdms compatibility tests on Windows

echo ===================================
echo TDMS nptdms Compatibility Test Setup
echo ===================================
echo.

REM Check if Python virtual environment exists
if not exist ".venv\" (
    echo Virtual environment not found at .venv
    echo Creating virtual environment...
    python -m venv .venv
    echo Virtual environment created
) else (
    echo Virtual environment found
)

REM Activate virtual environment and install dependencies
echo.
echo Installing Python dependencies...
.venv\Scripts\pip install -q --upgrade pip
.venv\Scripts\pip install -q nptdms numpy

echo Python dependencies installed
echo.

echo ===================================
echo Running Compatibility Tests
echo ===================================
echo.

REM Run the tests
if "%~1"=="" (
    echo Running all nptdms compatibility tests...
    cargo test --test nptdms_compatibility_tests
) else (
    echo Running test: %1
    cargo test --test nptdms_compatibility_tests %1
)

echo.
echo ===================================
echo Test run complete!
echo ===================================