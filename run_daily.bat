@echo off
chcp 65001 >nul
set PYTHONUTF8=1
cd /d D:\bg-city-cost-tracker
.venv\Scripts\python.exe src\run_pipeline.py >> logs\pipeline.log 2>&1
