package com.kafka.demo.kafkademo.controller;

import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import com.kafka.demo.kafkademo.producer.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    /**
     * 1. Kịch bản bình thường: Tạo 1 đơn hàng ngẫu nhiên.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(
            @RequestParam(required = false) String orderId,
            @RequestParam(defaultValue = "500000") BigDecimal amount
    ) {
        String finalOrderId = orderId != null ? orderId : "ORD-" + UUID.randomUUID().toString().substring(0, 8);
        OrderCreatedEvent event = new OrderCreatedEvent(
                finalOrderId,
                "CUST-" + (int)(Math.random() * 1000),
                amount,
                "VND",
                "CREATED",
                Instant.now()
        );

        orderProducer.publishOrder(event);

        return ResponseEntity.ok(Map.of(
                "status", "PUBLISHED",
                "message", "Order event sent to Kafka asynchronously",
                "orderId", finalOrderId,
                "amount", amount
        ));
    }

    /**
     * 2. Kịch bản kiểm chứng Key Ordering: Bắn 3 sự kiện của CÙNG 1 đơn hàng.
     */
    @PostMapping("/same-key")
    public ResponseEntity<Map<String, Object>> createSameKeyOrders(
            @RequestParam(defaultValue = "ORD-888") String key
    ) {
        for (String status : new String[]{"CREATED", "PAID", "DELIVERED"}) {
            OrderCreatedEvent event = new OrderCreatedEvent(
                    key,
                    "CUST-VIP-01",
                    BigDecimal.valueOf(1200000),
                    "VND",
                    status,
                    Instant.now()
            );
            orderProducer.publishOrder(event);
        }

        return ResponseEntity.ok(Map.of(
                "status", "PUBLISHED_3_EVENTS",
                "key", key,
                "note", "Kiểm tra Kafka UI/Console: Cả 3 event luôn rơi vào cùng 1 Partition!"
        ));
    }

    /**
     * 3. Kịch bản lỗi: Cố tình gửi số tiền âm để kích hoạt Retry và chuyển DLT.
     */
    @PostMapping("/error")
    public ResponseEntity<Map<String, Object>> createErrorOrder() {
        String errorOrderId = "ERR-" + UUID.randomUUID().toString().substring(0, 6);
        OrderCreatedEvent event = new OrderCreatedEvent(
                errorOrderId,
                "CUST-TEST",
                BigDecimal.valueOf(-99000), // Số tiền âm gây lỗi
                "VND",
                "INVALID",
                Instant.now()
        );

        orderProducer.publishOrder(event);

        return ResponseEntity.ok(Map.of(
                "status", "PUBLISHED_ERROR_ORDER",
                "orderId", errorOrderId,
                "note", "BillingConsumer sẽ retry 3 lần rồi chuyển sang topic order-events-dlt!"
        ));
    }

    /**
     * 4. Kịch bản Stress: Bắn 10 đơn hàng song song để xem chia tải qua 3 Partition.
     */
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> createBatchOrders() {
        for (int i = 1; i <= 10; i++) {
            String orderId = "BATCH-" + i;
            OrderCreatedEvent event = new OrderCreatedEvent(
                    orderId,
                    "CUST-" + i,
                    BigDecimal.valueOf(100000 + i * 10000),
                    "VND",
                    "CREATED",
                    Instant.now()
            );
            orderProducer.publishOrder(event);
        }

        return ResponseEntity.ok(Map.of(
                "status", "BATCH_PUBLISHED_10",
                "note", "10 đơn hàng đã được phân bổ đều ra 3 Partition."
        ));
    }
}
