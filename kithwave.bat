@echo off
setlocal EnableExtensions DisableDelayedExpansion

cd /d "%~dp0" || (
    echo [KithWave] Failed to switch to project directory: %~dp0
    exit /b 1
)

set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "PS_SCRIPT=%~dp0kithwave.ps1"
set "PID_FILE=%~dp0kithwave.pid"

if not exist "%PS_SCRIPT%" (
    echo [KithWave] Missing script: "%PS_SCRIPT%"
    exit /b 1
)

if "%~1"=="" goto :start
if /I "%~1"=="run" goto :run_console
if /I "%~1"=="start" goto :start
if /I "%~1"=="console" goto :run_console
if /I "%~1"=="stop" goto :stop
if /I "%~1"=="status" goto :status
if /I "%~1"=="restart" goto :restart
if /I "%~1"=="check" goto :check
if /I "%~1"=="menu" goto :menu
if /I "%~1"=="help" goto :help
if /I "%~1"=="-h" goto :help
if /I "%~1"=="/?" goto :help

echo [KithWave] Unknown command: %~1
echo.
goto :help_error

:start
call :invoke start
exit /b %errorlevel%

:run_console
call :invoke run
exit /b %errorlevel%

:stop
call :invoke stop
exit /b %errorlevel%

:status
call :invoke status
exit /b %errorlevel%

:restart
call :invoke restart
exit /b %errorlevel%

:check
set "HAS_WARN=0"
echo [KithWave] Environment check
echo ----------------------------------------

if exist ".\bot.py" (
    echo [OK] bot.py found.
) else (
    echo [WARN] bot.py is missing.
    set "HAS_WARN=1"
)

if exist ".\kithwave.ps1" (
    echo [OK] kithwave.ps1 found.
) else (
    echo [WARN] kithwave.ps1 is missing.
    set "HAS_WARN=1"
)

if exist ".\.venv\Scripts\python.exe" (
    echo [OK] .venv Python found.
) else (
    echo [WARN] .venv Python missing at .\.venv\Scripts\python.exe
    set "HAS_WARN=1"
)

if exist ".\.env" (
    echo [OK] .env found.
) else (
    echo [WARN] .env file missing.
    set "HAS_WARN=1"
)

set "TOKEN_LINE="
for /f "usebackq delims=" %%A in (`findstr /I /R "^DISCORD_TOKEN=.*" ".\.env" 2^>nul`) do (
    set "TOKEN_LINE=%%A"
    goto :token_done
)
:token_done
if defined TOKEN_LINE (
    echo [OK] DISCORD_TOKEN entry present in .env
) else (
    echo [WARN] DISCORD_TOKEN entry not found in .env
    set "HAS_WARN=1"
)

where ffmpeg >nul 2>&1
if errorlevel 1 (
    echo [WARN] ffmpeg not found in PATH.
    set "HAS_WARN=1"
) else (
    echo [OK] ffmpeg found in PATH.
)

if exist ".\kithwave.pid" (
    for /f "usebackq delims=" %%P in (".\kithwave.pid") do (
        if not "%%P"=="" (
            echo [INFO] PID file value: %%P
            goto :pid_done
        )
    )
)
echo [INFO] No PID file present.
goto :check_done

:pid_done

:check_done
if "%HAS_WARN%"=="0" (
    echo ----------------------------------------
    echo [KithWave] Check complete. No warnings.
) else (
    echo ----------------------------------------
    echo [KithWave] Check complete with warnings above.
)
exit /b 0

:menu
:menu_loop
cls
echo ==================================================
echo                    KithWave Control
echo ==================================================
echo Project: %CD%
echo.
call :invoke status
echo.
echo  1. Start bot
echo  2. Stop bot
echo  3. Restart bot
echo  4. Show status
echo  5. Environment check
echo  6. Interactive console
echo  Q. Quit
echo.
choice /C 123456Q /N /M "Select option: "
if errorlevel 7 exit /b 0
if errorlevel 6 call :run_console & call :pause_return & goto :menu_loop
if errorlevel 5 call :check & call :pause_return & goto :menu_loop
if errorlevel 4 call :status & call :pause_return & goto :menu_loop
if errorlevel 3 call :restart & call :pause_return & goto :menu_loop
if errorlevel 2 call :stop & call :pause_return & goto :menu_loop
if errorlevel 1 call :start & call :pause_return & goto :menu_loop
goto :menu_loop

:pause_return
echo.
pause
exit /b 0

:help
echo KithWave launcher
echo.
echo Usage:
echo   kithwave.bat start
echo   kithwave.bat console
echo   kithwave.bat stop
echo   kithwave.bat status
echo   kithwave.bat restart
echo   kithwave.bat check
echo   kithwave.bat menu
echo.
echo Notes:
echo   - No argument defaults to ^`start^` (non-interactive background start).
echo   - Use ^`console^` for interactive control mode.
exit /b 0

:help_error
call :help
exit /b 1

:invoke
set "MODE=%~1"
"%PS_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%" %MODE%
exit /b %errorlevel%
