package com.kafka.demo.kafkademo.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * ============================================================================
 * RECORD: OrderCreatedEvent
 * VAI TRÒ: Đối tượng truyền dữ liệu (Data Transfer Object / Event Payload).
 * 
 * ĐẶC ĐIỂM KỸ THUẬT:
 * - Sử dụng Java Record: Dữ liệu bất biến (immutable), tự động sinh constructor,
 *   getter (dưới dạng name()), equals, hashCode và toString.
 * - Triển khai Serializable để hỗ trợ tuần tự hóa dữ liệu.
 * 
 * CÁC TRƯỜNG DỮ LIỆU:
 * @param orderId    Mã định danh đơn hàng duy nhất (dùng làm Message Key trong Kafka).
 * @param customerId Mã định danh khách hàng đặt hàng.
 * @param amount     Tổng giá trị đơn hàng (dùng để mô phỏng dữ liệu hợp lệ hoặc lỗi âm tiền).
 * @param currency   Đơn vị tiền tệ (ví dụ: VND, USD).
 * @param status     Trạng thái vòng đời đơn hàng (ví dụ: CREATED, PAID, DELIVERED).
 * @param createdAt  Thời điểm tạo sự kiện.
 * ============================================================================
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
