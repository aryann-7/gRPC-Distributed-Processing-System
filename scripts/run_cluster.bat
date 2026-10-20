@echo off
setlocal enabledelayedexpansion
set "JAVA_TOOL_OPTIONS="

REM =========================================================================
REM CS324 Distributed Computing Cluster Build and Run Script (Windows)
REM =========================================================================
REM Usage:
REM   run_cluster.bat [num_workers] [num_clients]
REM Examples:
REM   run_cluster.bat         (defaults to 3 workers, 2 clients)
REM   run_cluster.bat 5       (5 workers, 2 clients)
REM   run_cluster.bat 6 3     (6 workers, 3 clients)
REM =========================================================================

set NUM_WORKERS=%1
if "%NUM_WORKERS%"=="" set NUM_WORKERS=3

set NUM_CLIENTS=%2
if "%NUM_CLIENTS%"=="" set NUM_CLIENTS=2

REM Validate worker bounds (between 3 and 10 as per demo requirements)
if %NUM_WORKERS% LSS 3 (
    echo [WARNING] Requested %NUM_WORKERS% workers. Minimum recommended is 3. Adjusting to 3.
    set NUM_WORKERS=3
)
if %NUM_WORKERS% GTR 10 (
    echo [WARNING] Requested %NUM_WORKERS% workers. Maximum supported in demo range is 10. Adjusting to 10.
    set NUM_WORKERS=10
)

echo =========================================================================
echo Cluster Configuration: %NUM_WORKERS% Workers (IDs 101-%NUM_WORKERS%), %NUM_CLIENTS% Clients
echo =========================================================================

echo [1/5] Building project packages via Maven...
call mvn clean package "-DskipTests=true"
if %ERRORLEVEL% NEQ 0 (
    echo Maven build failed!
    exit /b %ERRORLEVEL%
)

echo Building client package...
cd client
call mvn clean package "-DskipTests=true"
cd ..
if %ERRORLEVEL% NEQ 0 (
    echo Client build failed!
    exit /b %ERRORLEVEL%
)
echo Build successful.

if exist "%~dp0clients_registry.txt" del /f /q "%~dp0clients_registry.txt" 2>nul

set JVM_FLAGS=--add-opens java.base/jdk.internal.misc=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true

echo [2/5] Launching Bootstrap gRPC Server on port 50050...
start "Bootstrap gRPC Server" java %JVM_FLAGS% -jar target\bootstrap-server.jar 50050

timeout /t 3 /nobreak >nul

echo [3/5] Launching %NUM_WORKERS% Worker Nodes dynamically...
for /L %%i in (1,1,%NUM_WORKERS%) do (
    set /a W_ID=100 + %%i
    set /a W_RMI=1100 + %%i
    set /a W_GRPC=50050 + %%i
    echo Starting Worker-!W_ID! on port !W_GRPC!...
    start "Worker-!W_ID!" java %JVM_FLAGS% -jar target\worker-node.jar !W_ID! localhost !W_RMI! localhost 50050 !W_GRPC!
    echo !W_ID! > "%~dp0cluster_state.txt"
    timeout /t 1 /nobreak >nul
)

timeout /t 3 /nobreak >nul

echo [4/5] Launching %NUM_CLIENTS% Client GUI instances connected to Bootstrap (50050)...
for /L %%c in (1,1,%NUM_CLIENTS%) do (
    echo Starting Client GUI #%%c...
    start "Client GUI #%%c" java %JVM_FLAGS% -jar client\target\distrilab-client.jar %%c localhost 50050
    echo %%c > "%~dp0client_state.txt"
    timeout /t 1 /nobreak >nul
)

echo =========================================================================
echo gRPC Cluster launched successfully with %NUM_WORKERS% workers and %NUM_CLIENTS% clients.
echo =========================================================================
