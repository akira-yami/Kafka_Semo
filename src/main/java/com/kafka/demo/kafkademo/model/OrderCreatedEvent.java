package com.kafka.demo.kafkademo.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Event đại diện cho sự kiện một đơn hàng mới được tạo trong hệ thống.
 * Java Record tự động sinh constructor, getter, equals, hashCode, toString.
 */
public record OrderCreatedEvent(
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency,
        String status,
        Instant createdAt
) implements Serializable {
}
