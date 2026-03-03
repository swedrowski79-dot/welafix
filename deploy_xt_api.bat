@echo off
setlocal

REM Usage: deploy_xt_api.bat "C:\path\to\afs_api"
if "%~1"=="" (
  echo Usage: %~nx0 "C:\path\to\afs_api"
  exit /b 1
)

set "TARGET=%~1"
if not exist "%TARGET%" (
  echo Target folder does not exist: %TARGET%
  exit /b 1
)

if not exist "%TARGET%\xt_api" (
  mkdir "%TARGET%\xt_api"
)

echo Copying xt_api to %TARGET%\xt_api ...
robocopy "%~dp0xt_api" "%TARGET%\xt_api" /E /PURGE /NFL /NDL /NJH /NJS

if %ERRORLEVEL% GEQ 8 (
  echo Robocopy error level %ERRORLEVEL%
  exit /b %ERRORLEVEL%
)

echo Done.
exit /b 0
