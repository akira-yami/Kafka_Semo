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

/**
 * ============================================================================
 * CLASS: KafkaConfig
 * VAI TRÒ: Trung tâm cấu hình hạ tầng Spring Kafka.
 * 
 * NHIỆM VỤ CHÍNH:
 * 1. Khởi tạo Topics (Topics Initialization): Tạo topic nghiệp vụ & DLT.
 * 2. Cấu hình Producer Factory & Serializer linh hoạt đa kiểu dữ liệu.
 * 3. Cấu hình Error Handler (Retry logic & Dead Letter Publishing).
 * 4. Cấu hình Container Factory tùy chỉnh hỗ trợ xác nhận thủ công (Manual Ack).
 * ============================================================================
 */
@Configuration
public class KafkaConfig {

    // Tên topic chính nhận các sự kiện đơn hàng
    public static final String TOPIC_ORDER_EVENTS = "order-events";

    // Tên Dead Letter Topic (DLT) tiếp nhận các tin nhắn bị lỗi sau khi hết lượt retry
    public static final String TOPIC_ORDER_EVENTS_DLT = "order-events-dlt";

    /**
     * BEAN: orderEventsTopic
     * NHIỆM VỤ: Tự động khởi tạo topic 'order-events' trên Kafka Broker khi ứng dụng khởi động.
     * - Số partitions = 3: Cho phép chia tải và xử lý song song trên 3 luồng/consumer.
     * - Replication factor = 1: Phù hợp với môi trường local/demo gồm 1 node broker.
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(TOPIC_ORDER_EVENTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * BEAN: orderEventsDltTopic
     * NHIỆM VỤ: Tự động khởi tạo Dead Letter Topic 'order-events-dlt'.
     * - Số partitions = 1: Đủ để gom toàn bộ tin nhắn lỗi (Poison Pills) phục vụ việc điều tra.
     * - Replication factor = 1.
     */
    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(TOPIC_ORDER_EVENTS_DLT)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * BEAN: kafkaProducerFactory
     * NHIỆM VỤ: Thiết lập nhà máy sản xuất Producer, cho phép tuần tự hóa (serialize) 
     * linh hoạt dựa trên kiểu dữ liệu của đối tượng:
     * - Đối tượng OrderCreatedEvent -> JsonSerializer (chuyển thành JSON string).
     * - Mảng byte[] -> ByteArraySerializer (giữ nguyên byte thô khi cần đẩy qua DLT).
     */
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
     * BEAN: defaultErrorHandler
     * NHIỆM VỤ: Xử lý ngoại lệ toàn cục cho các Consumer Listeners:
     * 1. Cơ chế Retry: FixedBackOff(1000L, 2L) -> Thử lại tối đa 2 lần nữa (tổng cộng 3 lần gọi),
     *    mỗi lần cách nhau 1000ms (1 giây).
     * 2. Cơ chế Dead Letter Topic: Nếu sau 3 lần vẫn lỗi, DeadLetterPublishingRecoverer sẽ
     *    đẩy bản ghi sang topic 'order-events-dlt' (partition 0) kèm theo thông tin lỗi trong header.
     * 3. handler.setCommitRecovered(true): Đảm bảo sau khi đã đẩy sang DLT an toàn, offset của bản ghi
     *    trên topic gốc sẽ được commit để tránh nghẽn dòng dữ liệu tiếp theo.
     */
    @Bean
    public DefaultErrorHandler defaultErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(TOPIC_ORDER_EVENTS_DLT, 0)
        );

        // Ném lỗi nếu việc gửi sang DLT gặp thất bại
        recoverer.setFailIfSendResultIsError(true);

        // Thử lại 2 lần, mỗi lần cách nhau 1000ms
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        
        // Đánh dấu đã xử lý xong để commit offset sau khi chuyển DLT thành công
        handler.setCommitRecovered(true);
        return handler;
    }

    /**
     * BEAN: billingKafkaListenerContainerFactory
     * NHIỆM VỤ: Tạo Container Factory riêng biệt dành cho BillingConsumer:
     * - Thiết lập AckMode = MANUAL_IMMEDIATE: Cho phép BillingConsumer chủ động gọi 
     *   ack.acknowledge() ngay khi hoàn tất giao dịch thanh toán.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> billingKafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        // Bật cơ chế xác nhận thủ công ngay lập tức
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
