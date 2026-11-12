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

@Service
public class EmailConsumer {

    private static final Logger log = LoggerFactory.getLogger(EmailConsumer.class);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS,
            groupId = "email-group"
    )
    public void onOrderCreated(
            @Payload OrderCreatedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("[EmailConsumer - email-group] Nhận Order=[{}] từ Partition=[{}], Offset=[{}]. Đang gửi email xác nhận cho Khách=[{}]...",
                event.orderId(), partition, offset, event.customerId());
    }
}
