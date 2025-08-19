@echo off
set BASEDIR=%~dp0

if exist "%BASEDIR%stop.flag" del "%BASEDIR%stop.flag"

"%BASEDIR%venv\Scripts\python.exe" "%BASEDIR%pipline.py"

exit
