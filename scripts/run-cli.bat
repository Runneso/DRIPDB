@echo off
setlocal EnableExtensions


set "ROOT_DIR=%~dp0.."
cd /d "%ROOT_DIR%" || exit /b 1

if "%HOST%"=="" set "HOST=127.0.0.1"
if "%PORT%"=="" set "PORT=54321"
if "%TRACE%"=="" set "TRACE=false"

set "ARGS=--host %HOST% --port %PORT%"
if /I "%TRACE%"=="true" set "ARGS=%ARGS% --trace"
if not "%FILE%"=="" set "ARGS=%ARGS% --file \"%FILE%\""

if not exist "db-cli\build\install\db-cli\bin\db-cli.bat" (
  call gradlew.bat --no-daemon :db-cli:installDist || exit /b 1
)

call "db-cli\build\install\db-cli\bin\db-cli.bat" %ARGS%


