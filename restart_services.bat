@echo off
REM Restart script for Windows when SparkContext errors occur

echo 🔄 Restarting Spark Data Pipeline Services...

echo 📋 Step 1: Stopping all services...
docker-compose -f docker-compose.yml -f docker-compose.override.yml down

echo 📋 Step 2: Cleaning up...
docker system prune -f

echo 📋 Step 3: Restarting services...
call start_services.bat

echo ✅ Restart complete!
pause
