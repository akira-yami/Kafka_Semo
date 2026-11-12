@echo off
chcp 65001 >nul
setlocal
set "BASE_URL=http://localhost:8082"

echo ================================================================
echo  KICH BAN 4: BATCH 10 DON - CHIA TAI QUA 3 PARTITION
echo ================================================================
echo  Ban 10 don (BATCH-1..10). Moi orderId khac nhau -^> trai deu 3 partition.
echo  concurrency=3 -^> 3 worker thread xu ly song song.
echo ----------------------------------------------------------------
echo.

curl -s -X POST "%BASE_URL%/api/orders/batch"

echo.
echo.
echo ----------------------------------------------------------------
echo  QUAN SAT:
echo   - Log app: 10 don BATCH-1..10 trai tren Partition=[0/1/2],
echo     xu ly boi cac thread container#0-x khac nhau.
echo   - Kafka UI: topic order-events -^> message phan bo deu 3 partition.
echo ================================================================
echo.
pause
endlocal
