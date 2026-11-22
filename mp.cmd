@echo off
rem NYC MobilityPulse - run tasks from cmd.exe or PowerShell:  mp help
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0mp.ps1" %*
exit /b %ERRORLEVEL%
