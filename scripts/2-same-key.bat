@echo off
chcp 65001 >nul
setlocal
set "BASE_URL=http://localhost:8082"
set "KEY=ORD-888"

echo ================================================================
echo  KICH BAN 2: ORDERING THEO KEY - 3 EVENT CUNG 1 PARTITION
echo ================================================================
echo  Ban 3 event cua cung don hang [%KEY%]: CREATED -^> PAID -^> DELIVERED.
echo  Cung key -^> Kafka bam vao cung 1 partition -^> dung thu tu.
echo ----------------------------------------------------------------
echo.

curl -s -X POST "%BASE_URL%/api/orders/same-key?key=%KEY%"

echo.
echo.
echo ----------------------------------------------------------------
echo  QUAN SAT:
echo   - Log app: ca 3 event [%KEY%] co cung Partition=[x], offset tang dan.
echo   - Kafka UI: topic order-events -^> loc key [%KEY%] -^> 3 message cung 1 partition.
echo ================================================================
echo.
pause
endlocal
