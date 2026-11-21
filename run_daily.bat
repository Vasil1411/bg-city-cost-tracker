@echo off
chcp 65001 >nul
set PYTHONUTF8=1
cd /d D:\bg-city-cost-tracker
if not exist logs mkdir logs
.venv\Scripts\python.exe -m src.run_pipeline >> logs\pipeline.log 2>&1