@echo off
setlocal EnableExtensions


set "ROOT_DIR=%~dp0.."
cd /d "%ROOT_DIR%" || exit /b 1

if "%PORT%"=="" set "PORT=54321"
if "%DATA_DIR%"=="" set "DATA_DIR=data"
if "%POOL_SIZE%"=="" set "POOL_SIZE=256"

if "%FLUSH_INTERVAL_MS%"=="" set "FLUSH_INTERVAL_MS=200"
if "%FLUSH_BATCH_SIZE%"=="" set "FLUSH_BATCH_SIZE=64"
if "%CHECKPOINT_INTERVAL_MS%"=="" set "CHECKPOINT_INTERVAL_MS=5000"

if not exist "db-server\build\install\db-server\bin\db-server.bat" (
  call gradlew.bat --no-daemon :db-server:installDist || exit /b 1
)

call "db-server\build\install\db-server\bin\db-server.bat" --port %PORT% --dataDir "%DATA_DIR%" --poolSize %POOL_SIZE% --flushIntervalMs %FLUSH_INTERVAL_MS% --flushBatchSize %FLUSH_BATCH_SIZE% --checkpointIntervalMs %CHECKPOINT_INTERVAL_MS%


