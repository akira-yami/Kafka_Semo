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
            containerFactory = "billingKafkaListenerContainerFactory"
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

        ack.acknowledge();
        log.info("[BillingConsumer SUCCESS] Mô phỏng thanh toán Order=[{}], đã gọi ack.acknowledge() cho Partition=[{}], Offset=[{}].",
                event.orderId(), partition, offset);
    }
}
