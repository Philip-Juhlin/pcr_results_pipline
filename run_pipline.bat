@echo off
REM Clear previous stop file
if exist E:\pcr_results_pipline\stop.flag del E:\pcr_results_pipline\stop.flag

REM Activate venv
E:\pcr_results_pipline\p\venv\Scripts\activate.bat

REM Run pipeline
python E:\pcr_results_pipline\pipline.py

REM Exit
exit
