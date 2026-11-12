@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0demo.ps1" -Scenario SameKey %*
exit /b %ERRORLEVEL%
