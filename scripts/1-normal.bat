@echo off
chcp 65001 >nul
setlocal
set "BASE_URL=http://localhost:8082"

echo ================================================================
echo  KICH BAN 1: LUONG BINH THUONG + FAN-OUT 3 CONSUMER GROUP
echo ================================================================
echo  Tao 1 don hang hop le (amount=500000).
echo  1 message -^> 3 group cung nhan: email-group, inventory-group, billing-group
echo ----------------------------------------------------------------
echo.

curl -s -X POST "%BASE_URL%/api/orders?amount=500000"

echo.
echo.
echo ----------------------------------------------------------------
echo  QUAN SAT:
echo   - Log app: cung orderId xuat hien o EmailConsumer, InventoryConsumer,
echo     BillingConsumer + dong "BillingConsumer SUCCESS ... ack.acknowledge()".
echo   - Kafka UI: topic order-events co them 1 message.
echo ================================================================
echo.
pause
endlocal
