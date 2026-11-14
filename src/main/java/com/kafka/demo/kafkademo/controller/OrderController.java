package com.kafka.demo.kafkademo.controller;

import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import com.kafka.demo.kafkademo.producer.OrderProducer;
import com.kafka.demo.kafkademo.producer.OrderProducer.PublishedOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * ============================================================================
 * CONTROLLER: OrderController
 * VAI TRÒ: Tầng Web REST API tiếp nhận yêu cầu từ người dùng và kích hoạt các kịch bản demo.
 * 
 * CÁC ENDPOINT DEMO:
 * 1. POST /api/orders           -> Kịch bản đơn hàng thông thường (Normal).
 * 2. POST /api/orders/same-key  -> Kịch bản bảo toàn thứ tự với cùng một Key (Same Key Ordering).
 * 3. POST /api/orders/error     -> Kịch bản lỗi số tiền âm để kích hoạt Retry & DLT.
 * 4. POST /api/orders/batch     -> Kịch bản gửi 10 đơn hàng để quan sát phân bổ tải (Partitioning).
 * ============================================================================
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    /**
     * KỊCH BẢN 1: TẠO MỘT ĐƠN HÀNG BÌNH THƯỜNG
     * Mô tả: Tạo 1 sự kiện đơn hàng ngẫu nhiên, kiểm tra tiền > 0 rồi bắn vào Kafka.
     * Mục đích quan sát: Cùng 1 orderId sẽ xuất hiện độc lập trong log của cả 3 Consumer (Fan-out).
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(
            @RequestParam(required = false) String orderId,
            @RequestParam(defaultValue = "500000") BigDecimal amount
    ) {
        // Kiểm tra hợp lệ: Số tiền đơn hàng không được âm
        if (amount.signum() < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "amount must be non-negative");
        }
        
        // Tự động sinh ID đơn hàng nếu client không truyền
        String finalOrderId = orderId != null ? orderId : "ORD-" + UUID.randomUUID();
        validateKey(finalOrderId);

        // Đóng gói sự kiện
        OrderCreatedEvent event = new OrderCreatedEvent(
                finalOrderId,
                "CUST-" + (int)(Math.random() * 1000),
                amount,
                "VND",
                "CREATED",
                Instant.now()
        );

        // Xuất bản sự kiện và trả phản hồi
        return publish(List.of(event), "PUBLISHED", Map.of(
                "orderId", finalOrderId,
                "amount", amount
        ));
    }

    /**
     * KỊCH BẢN 2: KIỂM CHỨNG BẢO TOÀN THỨ TỰ SỰ KIỆN THEO KEY (KEY ORDERING)
     * Mô tả: Bắn liên tiếp 3 sự kiện của CÙNG 1 đơn hàng (CREATED -> PAID -> DELIVERED).
     * Mục đích quan sát: Cả 3 sự kiện đều rơi vào CÙNG 1 PARTITION với OFFSET TĂNG DẦN.
     */
    @PostMapping("/same-key")
    public ResponseEntity<Map<String, Object>> createSameKeyOrders(
            @RequestParam(defaultValue = "ORD-888") String key
    ) {
        validateKey(key);

        // Tạo chuỗi 3 trạng thái liên tiếp của cùng 1 đơn hàng
        List<OrderCreatedEvent> events = List.of("CREATED", "PAID", "DELIVERED").stream()
                .map(status -> new OrderCreatedEvent(key, "CUST-VIP-01", BigDecimal.valueOf(1200000),
                        "VND", status, Instant.now()))
                .toList();

        return publish(events, "PUBLISHED_3_EVENTS", Map.of("key", key));
    }

    /**
     * KỊCH BẢN 3: KỊCH BẢN LỖI (RETRY & DEAD LETTER TOPIC)
     * Mô tả: Cố tình gửi đơn hàng với số tiền âm (-99,000 VND).
     * Mục đích quan sát: 
     * - EmailConsumer và InventoryConsumer vẫn nhận và xử lý bình thường.
     * - BillingConsumer phát hiện tiền âm -> ném lỗi -> Spring Kafka Retry 2 lần nữa.
     * - Sau 3 lần thất bại, bản ghi được đẩy sang topic 'order-events-dlt' và DltConsumer bắt được.
     */
    @PostMapping("/error")
    public ResponseEntity<Map<String, Object>> createErrorOrder() {
        String errorOrderId = "ERR-" + UUID.randomUUID();
        
        // Cố tình đặt số tiền âm để kích hoạt logic ném lỗi trong BillingConsumer
        OrderCreatedEvent event = new OrderCreatedEvent(
                errorOrderId,
                "CUST-TEST",
                BigDecimal.valueOf(-99000),
                "VND",
                "INVALID",
                Instant.now()
        );

        return publish(List.of(event), "PUBLISHED_ERROR_ORDER", Map.of(
                "orderId", errorOrderId,
                "note", "Billing thử xử lý tối đa 3 lần, sau đó chuyển sang order-events-dlt."
        ));
    }

    /**
     * KỊCH BẢN 4: GỬI HÀNG LOẠT (BATCH 10 ĐƠN HÀNG)
     * Mô tả: Gửi 10 đơn hàng với các key cố định từ BATCH-1 đến BATCH-10.
     * Mục đích quan sát: Xem cách Kafka dùng thuật toán băm (hashing) để chia đều tải qua 3 Partitions.
     */
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> createBatchOrders() {
        List<OrderCreatedEvent> events = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            String orderId = "BATCH-" + i;
            events.add(new OrderCreatedEvent(
                    orderId,
                    "CUST-" + i,
                    BigDecimal.valueOf(100000 + i * 10000),
                    "VND",
                    "CREATED",
                    Instant.now()
            ));
        }

        return publish(events, "BATCH_PUBLISHED_10", Map.of());
    }

    /**
     * HÀM DÙNG CHUNG: publish
     * NHIỆM VỤ: Lặp qua danh sách các sự kiện, gọi OrderProducer và tổng hợp kết quả trả về cho Client.
     * 
     * - Nếu Kafka ghi thành công: Trả về HTTP 200 OK kèm danh sách records (partition, offset).
     * - Nếu Kafka bị ngắt quãng hoặc lỗi: Trả về HTTP 503 SERVICE_UNAVAILABLE kèm chi tiết bản ghi bị lỗi.
     */
    private ResponseEntity<Map<String, Object>> publish(
            List<OrderCreatedEvent> events, String successStatus, Map<String, Object> details
    ) {
        List<PublishedOrder> records = new ArrayList<>();
        Map<String, Object> response = new LinkedHashMap<>();
        response.putAll(details);
        response.put("requestedCount", events.size());
        response.put("records", records);

        // Lần lượt gửi từng sự kiện vào Producer
        for (OrderCreatedEvent event : events) {
            try {
                records.add(orderProducer.publishOrder(event));
            } catch (KafkaException | org.apache.kafka.common.KafkaException ex) {
                log.warn("Kafka không thể xác nhận bản ghi key={} status={} sau khi đã ghi được {} bản ghi",
                        event.orderId(), event.status(), records.size(), ex);
                response.put("status", records.isEmpty() ? "PUBLISH_UNCONFIRMED" : "PARTIALLY_PUBLISHED");
                response.put("unconfirmedRecord", Map.of("orderId", event.orderId(), "eventStatus", event.status()));
                
                // Trả về mã HTTP 503 khi broker gặp lỗi không thể ghi nhận
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
            }
        }

        // Toàn bộ các bản ghi đã được Kafka Broker ghi nhận an toàn
        response.put("status", successStatus);
        
        // TRẢ VỀ HTTP 200 OK
        return ResponseEntity.ok(response);
    }

    /**
     * HÀM KIỂM TRA ĐỘ HỢP LỆ CỦA KEY
     */
    private void validateKey(String key) {
        if (key.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Order key must not be blank");
        }
    }
}
