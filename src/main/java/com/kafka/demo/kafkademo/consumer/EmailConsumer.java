package com.kafka.demo.kafkademo.consumer;

import com.kafka.demo.kafkademo.config.KafkaConfig;
import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * ============================================================================
 * CONSUMER: EmailConsumer
 * VAI TRÒ: Dịch vụ gửi Email thông báo (Notification / Email Service).
 * THUỘC GROUP: "email-group" (Mô hình Fan-out độc lập).
 * 
 * NHIỆM VỤ:
 * - Lắng nghe các sự kiện từ topic 'order-events'.
 * - Nhận thông tin đơn hàng và mô phỏng việc gửi email xác nhận cho khách hàng qua log.
 * - Hoạt động hoàn toàn độc lập: Nếu dịch vụ email bị trễ hay lỗi, không ảnh hưởng 
 *   đến tiến trình xử lý kho (Inventory) hay thanh toán (Billing).
 * ============================================================================
 */
@Service
public class EmailConsumer {

    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);

    /**
     * LẮNG NGHE SỰ KIỆN TẠO ĐƠN HÀNG
     * 
     * @param event     Payload đơn hàng đã được JsonDeserializer tự động giải mã.
     * @param partition Partition thực tế mà tin nhắn được lấy về (0, 1 hoặc 2).
     * @param offset    Vị trí số thứ tự của bản ghi trong partition đó.
     */
    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS,
            groupId = "email-group"
    )
    public void onOrderCreated(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        // Mô phỏng tác vụ gửi email xác nhận
        log.info("[EmailConsumer - email-group] Nhận Order=[{}] từ Partition=[{}], Offset=[{}]. Đang gửi email xác nhận cho Khách=[{}]...",
                event.orderId(), partition, offset, event.customerId());
    }
}
