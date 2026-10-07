@echo off
setlocal enabledelayedexpansion

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

echo [1/6] Cleaning previous build artifacts...
if exist bin rd /s /q bin
mkdir bin

echo [2/6] Compiling Java source files...
javac -d bin src\common\models\*.java src\common\interfaces\*.java src\bootstrap\*.java src\worker\election\*.java src\worker\computation\*.java src\worker\coordinator\*.java src\worker\*.java src\client\parser\*.java src\client\gui\*.java src\client\*.java
if %ERRORLEVEL% NEQ 0 (
    echo Compilation failed!
    exit /b %ERRORLEVEL%
)
echo Compilation successful.

echo [3/6] Starting RMI Registry on port 1099...
start "RMI Registry" rmiregistry -J-cp -Jbin 1099

timeout /t 2 /nobreak >nul

echo [4/6] Launching Bootstrap Server...
start "Bootstrap Server" java -cp bin bootstrap.BootstrapServer

timeout /t 2 /nobreak >nul

echo [5/6] Launching %NUM_WORKERS% Worker Nodes dynamically...
for /L %%i in (1,1,%NUM_WORKERS%) do (
    set /a W_ID=100 + %%i
    set /a W_PORT=1100 + %%i
    echo Starting Worker-!W_ID! on port !W_PORT!...
    start "Worker-!W_ID!" java -cp bin worker.WorkerMain !W_ID! localhost !W_PORT! localhost 1099
    timeout /t 1 /nobreak >nul
)

timeout /t 2 /nobreak >nul

echo [6/6] Launching %NUM_CLIENTS% Client GUI instances...
for /L %%c in (1,1,%NUM_CLIENTS%) do (
    echo Starting Client GUI #%%c...
    start "Client GUI #%%c" java -cp bin client.ClientMain %%c
    timeout /t 1 /nobreak >nul
)

echo =========================================================================
echo Cluster launched successfully with %NUM_WORKERS% workers and %NUM_CLIENTS% clients.
echo =========================================================================
