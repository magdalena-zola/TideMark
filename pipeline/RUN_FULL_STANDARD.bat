@echo off
if "%~2"=="" (
  echo Usage: RUN_FULL_STANDARD.bat "D:\AIS\ihs_baltic.csv" "D:\AIS\TideMark_full"
  exit /b 2
)
python run_pipeline.py "%~1" "%~2" --preset standard
