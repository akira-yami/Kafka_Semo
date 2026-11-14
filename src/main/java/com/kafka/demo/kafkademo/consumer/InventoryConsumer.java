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
 * CONSUMER: InventoryConsumer
 * VAI TRÒ: Dịch vụ Quản lý Kho hàng (Inventory / Warehouse Service).
 * THUỘC GROUP: "inventory-group" (Mô hình Fan-out độc lập).
 * 
 * NHIỆM VỤ:
 * - Lắng nghe các sự kiện từ topic 'order-events'.
 * - Nhận thông tin đơn hàng và mô phỏng việc kiểm tra hàng tồn, trừ số lượng tồn kho qua log.
 * - Quản lý offset riêng biệt với email-group và billing-group.
 * ============================================================================
 */
@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    /**
     * LẮNG NGHE SỰ KIỆN TẠO ĐƠN HÀNG ĐỂ CẬP NHẬT TỒN KHO
     * 
     * @param event     Payload đơn hàng.
     * @param partition Partition chứa bản ghi.
     * @param offset    Vị trí offset của bản ghi.
     */
    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS,
            groupId = "inventory-group"
    )
    public void onOrderCreated(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        // Mô phỏng trừ tồn kho hàng hóa
        log.info("[InventoryConsumer - inventory-group] Nhận Order=[{}] từ Partition=[{}], Offset=[{}]. Đang trừ số lượng tồn kho...",
                event.orderId(), partition, offset);
    }
}
