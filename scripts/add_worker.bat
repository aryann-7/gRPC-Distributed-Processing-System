@echo off
setlocal enabledelayedexpansion

REM =========================================================================
REM CS324 Add Dynamic Worker Script (Windows)
REM =========================================================================
REM Usage:
REM   add_worker.bat <workerId> [port] [bootstrapHost] [bootstrapPort]
REM Examples:
REM   add_worker.bat 104
REM   add_worker.bat 104 1104
REM   add_worker.bat 105 1105 localhost 1099
REM =========================================================================

set W_ID=%1
if "%W_ID%"=="" (
    echo Usage: add_worker.bat ^<workerId^> [port] [bootstrapHost] [bootstrapPort]
    echo Example: add_worker.bat 104 1104
    exit /b 1
)

set W_PORT=%2
if "%W_PORT%"=="" (
    REM Default port convention: 1000 + (workerId - 100) or 1000 + workerId
    if %W_ID% GEQ 100 (
        set /a W_PORT=1000 + %W_ID%
    ) else (
        set /a W_PORT=1100 + %W_ID%
    )
)

set BS_HOST=%3
if "%BS_HOST%"=="" set BS_HOST=localhost

set BS_PORT=%4
if "%BS_PORT%"=="" set BS_PORT=1099

echo =========================================================================
echo Dynamically joining Worker-%W_ID% to cluster...
echo Host: localhost, Port: %W_PORT%, Bootstrap: %BS_HOST%:%BS_PORT%
echo =========================================================================

start "Worker-%W_ID%" java -cp bin worker.WorkerMain %W_ID% localhost %W_PORT% %BS_HOST% %BS_PORT%

echo Worker-%W_ID% started in a new terminal window.
