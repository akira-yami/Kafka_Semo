# Kafka Demo

Spring Boot gửi sự kiện đơn hàng vào Kafka. Ba consumer group Email, Inventory và Billing nhận sự kiện độc lập. Email, tồn kho và thanh toán đều được mô phỏng qua log.

## Chạy demo trên Windows

Cần Docker Desktop đang chạy, `JAVA_HOME` trỏ tới JDK 25 và các cổng 8080, 8082, 9092 còn trống. Từ thư mục dự án, mở PowerShell và chạy:

```powershell
.\scripts\start-demo.bat
```

Script đợi Kafka sẵn sàng rồi chạy Spring Boot ở foreground. Khi log hiện `Started KafkaDemoApplication`, mở terminal thứ hai:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

API ở `http://localhost:8082/api/orders`. Mở [Kafka UI](http://localhost:8080) để xem topic, partition, offset và consumer group.

| Script | Kịch bản | Kết quả cần quan sát |
|---|---|---|
| `scripts\1-normal.bat` | Tạo một đơn | Cùng `orderId` xuất hiện trong log của cả ba consumer. |
| `scripts\2-same-key.bat` | Gửi `CREATED → PAID → DELIVERED` | Cùng partition, offset tăng dần. Mỗi lần chạy script sinh một key riêng. |
| `scripts\3-error-dlt.bat` | Gửi đơn có số tiền âm | Billing xử lý 1 lần rồi thử lại 2 lần, cách nhau 1 giây. Tìm `orderId` trong log `[DLT]` hoặc topic `order-events-dlt`. Email và Inventory vẫn nhận sự kiện. |
| `scripts\4-batch.bat` | Gửi 10 đơn | Script in số bản ghi thực tế ở từng partition. Batch dùng lại các key `BATCH-1` đến `BATCH-10` để dễ so sánh giữa các lần chạy. |

HTTP 200 xác nhận Kafka đã ghi các bản ghi liệt kê trong `records`. Consumer xử lý sau đó. Nếu một lần gửi thiếu xác nhận, API trả HTTP 503, liệt kê các bản ghi đã được xác nhận và bản ghi đang chờ xác nhận tại thời điểm lỗi. Các bản ghi còn lại của yêu cầu nhiều sự kiện chưa được gửi. Timeout có thể xảy ra sau khi broker đã ghi, vì vậy gửi lại cả yêu cầu có thể tạo bản ghi trùng.

Xem offset các consumer group:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --all-groups --describe
```

Dừng Spring Boot bằng Ctrl+C rồi chạy `docker compose down`. Volume `kafka-data` giữ bản ghi và offset qua lần khởi động tiếp theo. Nếu muốn bắt đầu một demo sạch, `docker compose down -v` sẽ xóa dữ liệu Kafka của project Compose này.

## Kiểm thử

```powershell
.\mvnw.cmd clean test
```

Bài test tích hợp dùng broker nhúng riêng. Các bài test kiểm tra xác nhận gửi, fan-out qua offset, thứ tự cùng key, batch, retry và DLT, JSON hỏng và lỗi gửi giữa chừng.
