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

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
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
        if (amount.signum() < 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "amount must be non-negative");
        }
        String finalOrderId = orderId != null ? orderId : "ORD-" + UUID.randomUUID();
        validateKey(finalOrderId);
        OrderCreatedEvent event = new OrderCreatedEvent(
                finalOrderId,
                "CUST-" + (int)(Math.random() * 1000),
                amount,
                "VND",
                "CREATED",
                Instant.now()
        );

        return publish(List.of(event), "PUBLISHED", Map.of(
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
        validateKey(key);
        List<OrderCreatedEvent> events = List.of("CREATED", "PAID", "DELIVERED").stream()
                .map(status -> new OrderCreatedEvent(key, "CUST-VIP-01", BigDecimal.valueOf(1200000),
                        "VND", status, Instant.now()))
                .toList();
        return publish(events, "PUBLISHED_3_EVENTS", Map.of("key", key));
    }

    /**
     * 3. Kịch bản lỗi: Cố tình gửi số tiền âm để kích hoạt Retry và chuyển DLT.
     */
    @PostMapping("/error")
    public ResponseEntity<Map<String, Object>> createErrorOrder() {
        String errorOrderId = "ERR-" + UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                errorOrderId,
                "CUST-TEST",
                BigDecimal.valueOf(-99000), // Số tiền âm gây lỗi
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
     * 4. Gửi 10 key cố định để dễ quan sát cách Kafka chia tải qua các partition.
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

    private ResponseEntity<Map<String, Object>> publish(
            List<OrderCreatedEvent> events, String successStatus, Map<String, Object> details
    ) {
        List<PublishedOrder> records = new ArrayList<>();
        Map<String, Object> response = new LinkedHashMap<>();
        response.putAll(details);
        response.put("requestedCount", events.size());
        response.put("records", records);

        for (OrderCreatedEvent event : events) {
            try {
                records.add(orderProducer.publishOrder(event));
            } catch (KafkaException | org.apache.kafka.common.KafkaException ex) {
                log.warn("Kafka did not confirm key={} status={} after {} confirmed record(s)",
                        event.orderId(), event.status(), records.size(), ex);
                response.put("status", records.isEmpty() ? "PUBLISH_UNCONFIRMED" : "PARTIALLY_PUBLISHED");
                response.put("unconfirmedRecord", Map.of("orderId", event.orderId(), "eventStatus", event.status()));
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
            }
        }

        response.put("status", successStatus);
        return ResponseEntity.ok(response);
    }

    private void validateKey(String key) {
        if (key.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Order key must not be blank");
        }
    }
}
