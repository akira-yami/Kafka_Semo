@echo off
chcp 65001 >nul
setlocal
set "BASE_URL=http://localhost:8082"

echo ================================================================
echo  KICH BAN 3: LOI -^> RETRY 3 LAN -^> DEAD LETTER TOPIC (DLT)
echo ================================================================
echo  Tao don hang so tien AM (-99000) -^> BillingConsumer nem loi.
echo  Co che: retry 2 lan (cach 1s), tong 3 lan xu ly, van fail -^> day sang
echo  topic order-events-dlt. Email/Inventory group van xu ly binh thuong.
echo ----------------------------------------------------------------
echo.

curl -s -X POST "%BASE_URL%/api/orders/error"

echo.
echo.
echo ----------------------------------------------------------------
echo  LUU Y: DLT can ~3 giay moi co message (do retry).
echo  QUAN SAT:
echo   - Log app: dong "PHAT HIEN LOI: So tien ... bi am" lap lai 3 LAN,
echo     sau do khoi "[DEAD LETTER TOPIC MONITOR] ..." voi ly do
echo     "Invalid negative payment amount: -99000".
echo   - Kafka UI: doi ~3s roi refresh topic order-events-dlt -^> co 1 message.
echo ================================================================
echo.
echo  Dang doi 4 giay de co che retry/DLT hoan tat...
timeout /t 4 /nobreak >nul
echo  Xong. Hay refresh Kafka UI (topic order-events-dlt) va xem log app.
echo.
pause
endlocal
