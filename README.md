# Kafka Demo - Hướng Dẫn Vận Hành & Chạy Dự Án

Hệ thống mô phỏng xử lý sự kiện đặt hàng (E-commerce Order Events) xây dựng trên nền tảng **Spring Boot 4.1.2** và **Apache Kafka 3.7.0 (KRaft mode)**.

---

## 1. Kiến Trúc & Các Thành Phần Chính

* **Kafka Broker (KRaft Mode)**: Chạy trên Docker container, không phụ thuộc Zookeeper.
  * Cổng `9092`: Kết nối từ ứng dụng Spring Boot trên máy host (`localhost:9092`).
  * Cổng `29092`: Mạng nội bộ Docker cho Kafka UI (`kafka:29092`).
* **Kafka UI**: Giao diện web trực quan tại [http://localhost:8080](http://localhost:8080) để giám sát topic, partition, offset và consumer lag.
* **Spring Boot Service** (Cổng `8082`):
  * **OrderProducer**: Đẩy sự kiện vào topic `order-events` (3 partitions), sử dụng `orderId` làm Message Key và đợi broker xác nhận (ACK).
  * **3 Consumer Groups độc lập (Mô hình Fan-out)**:
    1. `email-group` (`EmailConsumer`): Giả lập gửi email xác nhận.
    2. `inventory-group` (`InventoryConsumer`): Giả lập cập nhật và trừ số lượng kho.
    3. `billing-group` (`BillingConsumer`): Giả lập thanh toán (cấu hình `MANUAL_IMMEDIATE` Acknowledgment).
  * **Cơ chế Retry & Dead Letter Topic (DLT)**:
    * Khi số tiền đơn hàng âm (`amount < 0`), `BillingConsumer` ném lỗi giả lập.
    * Hệ thống kích hoạt `DefaultErrorHandler` thử lại 3 lần (1 lần gốc + 2 lần retry, cách nhau 1 giây).
    * Nếu vẫn thất bại, bản ghi được tự động chuyển sang topic `order-events-dlt` (1 partition) do `DltConsumer` (`dlt-monitor-group`) bắt và ghi log.

---

## 2. Yêu Cầu Tiền Đề (Prerequisites)

1. **Docker Desktop**: Đã cài đặt và đang ở trạng thái **Running** (Engine running).
2. **Java JDK 25**: Biến môi trường `JAVA_HOME` trỏ tới thư mục cài đặt JDK 25 (ví dụ: `C:\Program Files\Java\jdk-25`).
3. **Các cổng còn trống**:
   * `8080`: Kafka UI
   * `8082`: Spring Boot Application
   * `9092`: Kafka Broker

---

## 3. Các Bước Chạy Dự Án

### Bước 1: Bật Docker Desktop
Đảm bảo ứng dụng **Docker Desktop** trên máy đã được mở và engine đang chạy.

### Bước 2: Khởi động hệ thống (Kafka + Spring Boot)

Mở một terminal **PowerShell** tại thư mục gốc của dự án và chạy:

```powershell
.\scripts\start-demo.bat
```

> **Cơ chế hoạt động của script:**
> 1. Gọi `docker compose up -d --wait --wait-timeout 90` để khởi động Kafka và Kafka UI, chờ đến khi Kafka đạt trạng thái `healthy`.
> 2. Tự động gọi `mvnw.cmd spring-boot:run` để biên dịch và chạy Spring Boot ở foreground.

Chờ tới khi terminal hiển thị dòng thông báo sau là hệ thống đã sẵn sàng:
```text
Started KafkaDemoApplication in ... seconds
```

*(Hoặc nếu muốn chạy thủ công từng bước)*:
```powershell
# Khởi động Kafka & Kafka UI
docker compose up -d

# Chạy ứng dụng Spring Boot
.\mvnw.cmd spring-boot:run
```

---

### Bước 3: Truy Cập Giao Diện Giám Sát

* **Kafka UI**: Mở trình duyệt tại [http://localhost:8080](http://localhost:8080)
  * Kiểm tra 2 topics: `order-events` (3 partitions) và `order-events-dlt` (1 partition).
  * Kiểm tra danh sách Consumer Groups: `email-group`, `inventory-group`, `billing-group`, `dlt-monitor-group`.
* **API Endpoint**: `http://localhost:8082/api/orders`

---

### Bước 4: Chạy Các Kịch Bản Demo (Mở Terminal Thứ Hai)

Giữ nguyên terminal đang chạy Spring Boot ở Bước 2. Mở thêm một cửa sổ **PowerShell thứ hai** tại thư mục dự án.

#### Chạy toàn bộ các kịch bản demo:
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\demo.ps1
```

#### Hoặc chạy từng kịch bản riêng lẻ:

| Kịch bản | Lệnh thực hiện | Mục đích & Hiện tượng quan sát |
| :--- | :--- | :--- |
| **1. Đơn hàng chuẩn** | `.\scripts\1-normal.bat` | Tạo 1 đơn hàng bình thường. Quan sát log terminal thấy cùng một `orderId` được nhận độc lập bởi cả 3 consumer: `EmailConsumer`, `InventoryConsumer` và `BillingConsumer`. |
| **2. Thứ tự theo Key (Key Ordering)** | `.\scripts\2-same-key.bat` | Gửi 3 sự kiện liên tiếp của cùng 1 đơn hàng (`CREATED` → `PAID` → `DELIVERED`). Cả 3 sự kiện sẽ được gửi vào **cùng 1 partition** với **offset tăng dần liên tục**, đảm bảo tính tuần tự. |
| **3. Retry & Dead Letter Topic** | `.\scripts\3-error-dlt.bat` | Gửi đơn hàng có số tiền âm. `BillingConsumer` xử lý lỗi, thực hiện thử lại 2 lần (cách nhau 1 giây). Sau đó bản ghi được đẩy sang topic `order-events-dlt` và log `[DLT]` được in ra. Email và Inventory vẫn nhận đơn bình thường. |
| **4. Phân phối tải (Batch)** | `.\scripts\4-batch.bat` | Gửi loạt 10 đơn hàng với các key khác nhau. Script in thống kê số bản ghi được Kafka phân bổ đều vào 3 partitions. |

---

## 4. Các Lệnh Vận Hành & Kiểm Tra Nâng Cao

### Kiểm tra Offset và Consumer Lag:
Kiểm tra chi tiết vị trí đọc (Current Offset), Log End Offset và Lag của từng consumer group:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:29092 --all-groups --describe
```

### Chạy Kiểm Thử Tự Động (Integration Tests):
Dự án sử dụng broker Kafka nhúng (`@EmbeddedKafka`), có thể chạy test độc lập mà không cần Docker:
```powershell
.\mvnw.cmd clean test
```

---

## 5. Dừng Và Dọn Dẹp Môi Trường

1. **Dừng Spring Boot**: Nhấn tổ hợp phím `Ctrl + C` tại cửa sổ terminal đang chạy Spring Boot.
2. **Dừng Kafka và Kafka UI**:
   ```powershell
   docker compose down
   ```
3. **Xóa sạch dữ liệu (Reset demo về ban đầu)**:
   Mặc định volume `kafka-data` sẽ giữ lại dữ liệu các lần chạy trước. Nếu bạn muốn xóa toàn bộ message và offset để demo lại từ đầu:
   ```powershell
   docker compose down -v
   ```
