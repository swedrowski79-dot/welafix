@echo off

set "QUELLE=C:\Welafix_old\wela\xt_api"
set "ZIEL=C:\xampp\htdocs\afs_api"

robocopy "%QUELLE%" "%ZIEL%" /E /XO /R:2 /W:2

echo Fertig!
pause