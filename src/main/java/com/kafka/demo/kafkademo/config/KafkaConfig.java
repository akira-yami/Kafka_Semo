package com.kafka.demo.kafkademo.config;

import com.kafka.demo.kafkademo.model.OrderCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class KafkaConfig {

    public static final String TOPIC_ORDER_EVENTS = "order-events";
    public static final String TOPIC_ORDER_EVENTS_DLT = "order-events-dlt";

    /**
     * Tự động khởi tạo topic order-events với 3 partition ngay khi ứng dụng khởi động.
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(TOPIC_ORDER_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Tự động khởi tạo Dead Letter Topic (DLT) để cô lập các tin nhắn lỗi (Poison Pill).
     */
    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(TOPIC_ORDER_EVENTS_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public ProducerFactory<Object, Object> kafkaProducerFactory(KafkaProperties properties) {
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(),
                null,
                () -> new DelegatingByTypeSerializer(Map.of(
                        OrderCreatedEvent.class, new JsonSerializer<OrderCreatedEvent>(),
                        byte[].class, new ByteArraySerializer()
                )));
    }

    /**
     * Cấu hình Error Handler:
     * 1. Thử lại tối đa 3 lần (1 lần gốc + 2 lần retry), cách nhau 1000ms.
     * 2. Nếu vẫn thất bại, DeadLetterPublishingRecoverer đẩy sang topic order-events-dlt.
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(TOPIC_ORDER_EVENTS_DLT, 0)
        );

        recoverer.setFailIfSendResultIsError(true);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        handler.setCommitRecovered(true);
        return handler;
    }

    /**
     * Cấu hình Container Factory với Manual Acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> billingKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
