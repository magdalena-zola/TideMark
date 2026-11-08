@echo off
cd /d D:\AIS
python run_tidemark_sensitivity.py "D:\AIS\TideMark_full" --memory-limit 8GB --threads 4
pause
