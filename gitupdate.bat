@echo off
color 0A
echo ========================================
echo        WELAFIX GIT UPDATE TOOL
echo ========================================
echo.

:: Prüfen ob wir in einem Git Repository sind
git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
    echo ❌ Dieses Verzeichnis ist kein Git Repository!
    pause
    exit /b
)

:: Commit Nachricht abfragen
set /p commitmsg=Bitte Commit-Nachricht eingeben: 

if "%commitmsg%"=="" (
    echo ❌ Keine Commit-Nachricht eingegeben!
    pause
    exit /b
)

echo.
echo 🔄 Änderungen werden hinzugefügt...
git add .

echo 📝 Commit wird erstellt...
git commit -m "%commitmsg%"

echo 🚀 Push nach origin main...
git push origin main

echo.
echo ✅ Fertig!
pause
gitupdate.bat