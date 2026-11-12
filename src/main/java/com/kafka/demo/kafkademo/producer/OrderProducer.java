package com.kafka.demo.kafkademo.producer;

import com.kafka.demo.kafkademo.config.KafkaConfig;
import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Gửi OrderCreatedEvent lên Kafka bất đồng bộ.
     * Sử dụng orderId làm Key để đảm bảo tính toàn vẹn thứ tự (Message Ordering).
     */
    public CompletableFuture<SendResult<String, Object>> publishOrder(OrderCreatedEvent event) {
        String key = event.orderId();
        log.info("[Producer] Publishing order event with Key=[{}] to topic=[{}]...", key, KafkaConfig.TOPIC_ORDER_EVENTS);

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(KafkaConfig.TOPIC_ORDER_EVENTS, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                var metadata = result.getRecordMetadata();
                log.info("[Producer SUCCESS] Key=[{}] -> Partition=[{}] at Offset=[{}] (Timestamp: {})",
                        key, metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                log.error("[Producer FAILED] Could not send event with Key=[{}], error: {}", key, ex.getMessage(), ex);
            }
        });

        return future;
    }
}
