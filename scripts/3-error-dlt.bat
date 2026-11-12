@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0demo.ps1" -Scenario Retry %*
exit /b %ERRORLEVEL%
