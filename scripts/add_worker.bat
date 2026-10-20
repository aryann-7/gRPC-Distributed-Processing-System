@echo off
setlocal enabledelayedexpansion
set "JAVA_TOOL_OPTIONS="

REM =========================================================================
REM CS324 Add Dynamic Worker Script (Windows)
REM Automatically creates and connects one new worker without user input.
REM Auto-increments workerId and auto-assigns the next free gRPC port.
REM =========================================================================

set STATE_FILE=%~dp0cluster_state.txt
set BS_HOST=localhost
set BS_PORT=50050

REM 1. Determine base worker ID (default start after initial 3-worker cluster = 104)
set NEXT_ID=104
if exist "%STATE_FILE%" (
    set /p LAST_ID=<"%STATE_FILE%"
    REM Strip potential trailing spaces
    for /f "tokens=1" %%a in ("!LAST_ID!") do set LAST_ID=%%a
    if not "!LAST_ID!"=="" (
        set /a NEXT_ID=!LAST_ID! + 1
    )
)

REM 2. Allow optional CLI overrides if explicitly passed, otherwise use auto-calculated
if not "%1"=="" set NEXT_ID=%1

set W_ID=%NEXT_ID%
set /a W_RMI=1100 + %W_ID%
set /a W_GRPC=50050 + (%W_ID% - 100)

REM Optional argument overrides
if not "%2"=="" set W_GRPC=%2
if not "%3"=="" set BS_HOST=%3
if not "%4"=="" set BS_PORT=%4

REM 3. Persist the newly allocated worker ID so consecutive executions auto-increment
echo %W_ID% > "%STATE_FILE%"

echo =========================================================================
echo [Auto-Add Worker] Spawning Worker-%W_ID%...
echo Assigned Worker ID : %W_ID%
echo Internal Port      : %W_RMI%
echo gRPC Port          : %W_GRPC%
echo Connecting to      : %BS_HOST%:%BS_PORT%
echo =========================================================================

set JVM_FLAGS=--add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true
start "Worker-%W_ID%" java %JVM_FLAGS% -jar "%~dp0..\target\worker-node.jar" %W_ID% localhost %W_RMI% %BS_HOST% %BS_PORT% %W_GRPC%

echo Worker-%W_ID% started in a new terminal window.
