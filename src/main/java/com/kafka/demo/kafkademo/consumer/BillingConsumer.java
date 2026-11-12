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

@Service
public class BillingConsumer {

    private static final Logger log = LoggerFactory.getLogger(BillingConsumer.class);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS,
            groupId = "billing-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(
            @Payload OrderCreatedEvent event,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[BillingConsumer - billing-group] Received Order=[{}] from Partition=[{}] at Offset=[{}] with Amount=[{} {}]",
                event.orderId(), partition, offset, event.amount(), event.currency());

        // KỊCH BẢN GIẢ LẬP LỖI (ĐỂ DEMO RETRY & DEAD LETTER TOPIC)
        if (event.amount() != null && event.amount().compareTo(BigDecimal.ZERO) < 0) {
            log.error("[BillingConsumer] PHÁT HIỆN LỖI: Số tiền đơn hàng [{}] bị âm ({}). Ném ngoại lệ để kích hoạt Retry & DLT!",
                    event.orderId(), event.amount());
            throw new IllegalArgumentException("Invalid negative payment amount: " + event.amount());
        }

        // BƯỚC 1: XỬ LÝ NGHIỆP VỤ & LƯU VÀO DATABASE
        processPayment(event);

        // BƯỚC 2: CHỈ GỌI MANUAL ACK KHI NGHIỆP VỤ ĐÃ THÀNH CÔNG HOÀN TOÀN
        if (ack != null) {
            ack.acknowledge();
            log.info("[BillingConsumer SUCCESS] Đã trừ tiền thành công cho Order=[{}]. Gọi ack.acknowledge() để commit Offset=[{}].",
                    event.orderId(), offset);
        }
    }

    private void processPayment(OrderCreatedEvent event) {
        try {
            // Giả lập thời gian xử lý giao dịch thanh toán ngân hàng
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
