package com.kafka.demo.kafkademo.producer;

import com.kafka.demo.kafkademo.config.KafkaConfig;
import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderProducer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Dùng orderId làm key và đợi broker xác nhận trước khi trả kết quả.
     */
    public PublishedOrder publishOrder(OrderCreatedEvent event) {
        try {
            var metadata = kafkaTemplate.send(KafkaConfig.TOPIC_ORDER_EVENTS, event.orderId(), event)
                    .get(10, TimeUnit.SECONDS).getRecordMetadata();
            log.info("[Producer] key={} status={} partition={} offset={}",
                    event.orderId(), event.status(), metadata.partition(), metadata.offset());
            return new PublishedOrder(event.orderId(), event.status(), metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Interrupted while waiting for Kafka confirmation", ex);
        } catch (ExecutionException ex) {
            throw new KafkaException("Kafka did not confirm the record", ex.getCause());
        } catch (TimeoutException ex) {
            throw new KafkaException("Timed out waiting for Kafka confirmation", ex);
        }
    }

    public record PublishedOrder(String orderId, String eventStatus, String topic, int partition, long offset) {}
}
