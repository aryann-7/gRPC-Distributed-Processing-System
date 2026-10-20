@echo off
setlocal enabledelayedexpansion
set "JAVA_TOOL_OPTIONS="

REM =========================================================================
REM CS324 Add Dynamic Client GUI Script (Windows)
REM Automatically creates and connects one new Client GUI without user input.
REM Auto-increments clientId across consecutive runs and connects to Bootstrap.
REM =========================================================================

set STATE_FILE=%~dp0client_state.txt
set BS_HOST=localhost
set BS_PORT=50050

REM 1. Determine base client ID (default start after initial 2-client cluster = 3)
set NEXT_ID=3
if exist "%STATE_FILE%" (
    set /p LAST_ID=<"%STATE_FILE%"
    for /f "tokens=1" %%a in ("!LAST_ID!") do set LAST_ID=%%a
    if not "!LAST_ID!"=="" (
        set /a NEXT_ID=!LAST_ID! + 1
    )
)

REM 2. Allow optional CLI overrides if explicitly passed, otherwise use auto-calculated
if not "%1"=="" set NEXT_ID=%1
if not "%2"=="" set BS_HOST=%2
if not "%3"=="" set BS_PORT=%3

set CLIENT_ID=%NEXT_ID%

REM 3. Persist the newly allocated client ID so consecutive executions auto-increment
echo %CLIENT_ID% > "%STATE_FILE%"

echo =========================================================================
echo [Auto-Add Client] Spawning Client GUI #%CLIENT_ID%...
echo Assigned Client ID : #%CLIENT_ID%
echo Connecting to      : %BS_HOST%:%BS_PORT%
echo =========================================================================

set JVM_FLAGS=--add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true
start "Client GUI #%CLIENT_ID%" java %JVM_FLAGS% -jar "%~dp0..\client\target\distrilab-client.jar" %CLIENT_ID% %BS_HOST% %BS_PORT%

echo Client GUI #%CLIENT_ID% started in a new window.
