package com.kafka.demo.kafkademo.consumer;

import com.kafka.demo.kafkademo.config.KafkaConfig;
import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * ============================================================================
 * CONSUMER: BillingConsumer
 * VAI TRÒ: Dịch vụ Xử lý Thanh toán (Billing / Payment Service).
 * THUỘC GROUP: "billing-group" (Mô hình Fan-out độc lập).
 * 
 * ĐẶC ĐIỂM KỸ THUẬT QUAN TRỌNG:
 * 1. Xác nhận thủ công (Manual Acknowledgment): Dùng container factory riêng
 *    'billingKafkaListenerContainerFactory' để có quyền gọi ack.acknowledge() sau khi hoàn tất.
 * 2. Kịch bản mô phỏng lỗi (Poison Pill):
 *    - Nếu số tiền amount < 0 -> ném IllegalArgumentException.
 *    - Kích hoạt DefaultErrorHandler tự động thử lại (Retry) 2 lần nữa.
 *    - Nếu vẫn thất bại -> chuyển bản ghi sang Dead Letter Topic 'order-events-dlt'.
 * ============================================================================
 */
@Service
public class BillingConsumer {

    private static final Logger log = LoggerFactory.getLogger(BillingConsumer.class);

    /**
     * LẮNG NGHE SỰ KIỆN ĐƠN HÀNG ĐỂ TIẾN HÀNH THANH TOÁN
     * 
     * @param event     Payload đơn hàng cần thanh toán.
     * @param ack       Đối tượng Acknowledgment dùng để báo cho Kafka biết đã xử lý xong.
     * @param partition Partition của bản ghi.
     * @param offset    Vị trí offset của bản ghi.
     */
    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS,
            groupId = "billing-group",
            containerFactory = "billingKafkaListenerContainerFactory"
    )
    public void onOrderCreated(
            @Payload OrderCreatedEvent event,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[BillingConsumer - billing-group] Nhận Order=[{}] từ Partition=[{}] tại Offset=[{}] với số tiền=[{} {}]",
                event.orderId(), partition, offset, event.amount(), event.currency());

        // ========================================================================
        // KỊCH BẢN GIẢ LẬP LỖI (ĐỂ DEMO CƠ CHẾ RETRY & DEAD LETTER TOPIC)
        // ========================================================================
        if (event.amount() != null && event.amount().compareTo(BigDecimal.ZERO) < 0) {
            log.error("[BillingConsumer] PHÁT HIỆN LỖI: Số tiền đơn hàng [{}] bị âm ({}). Ném ngoại lệ để kích hoạt Retry & DLT!",
                    event.orderId(), event.amount());
            // Ném ngoại lệ: DefaultErrorHandler sẽ chặn lại, không gọi ack, và tiến hành Retry
            throw new IllegalArgumentException("Số tiền thanh toán bị âm không hợp lệ: " + event.amount());
        }

        // ========================================================================
        // KỊCH BẢN THÀNH CÔNG: XÁC NHẬN COMMIT OFFSET
        // ========================================================================
        // Xác nhận với Kafka Broker rằng bản ghi này đã được xử lý xong an toàn
        ack.acknowledge();
        
        log.info("[BillingConsumer SUCCESS] Mô phỏng thanh toán thành công Order=[{}], đã gọi ack.acknowledge() cho Partition=[{}], Offset=[{}].",
                event.orderId(), partition, offset);
    }
}
