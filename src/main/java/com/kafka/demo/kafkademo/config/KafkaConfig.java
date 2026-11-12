package com.kafka.demo.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

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

        // Thử lại 2 lần (khoảng cách 1000ms), tổng cộng 3 lần xử lý
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2L);
        return new DefaultErrorHandler(recoverer, fixedBackOff);
    }

    /**
     * Cấu hình Container Factory với Manual Acknowledgment.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler defaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.setConcurrency(3); // 3 worker threads tương ứng 3 partitions
        return factory;
    }
}
