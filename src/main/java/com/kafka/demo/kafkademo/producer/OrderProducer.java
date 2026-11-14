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

/**
 * ============================================================================
 * SERVICE: OrderProducer
 * VAI TRÒ: Tầng dịch vụ đảm nhiệm việc gửi các sự kiện đơn hàng vào Kafka Broker.
 * 
 * ĐẶC ĐIỂM KỸ THUẬT:
 * 1. Đồng bộ hóa xác nhận (Synchronous ACK): Sử dụng .get(timeout) để đợi phản hồi 
 *    từ Kafka Broker nhằm đảm bảo message đã được ghi đĩa an toàn trước khi báo về Controller.
 * 2. Phân vùng theo Key (Key-based Partitioning): Dùng orderId làm Key để mọi sự kiện
 *    của cùng 1 đơn hàng luôn rơi vào cùng 1 Partition.
 * ============================================================================
 */
@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderProducer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * PHƯƠNG THỨC: publishOrder
     * NHIỆM VỤ: Gửi sự kiện OrderCreatedEvent vào topic 'order-events'.
     * 
     * @param event Sự kiện đơn hàng cần gửi.
     * @return PublishedOrder Đối tượng chứa thông tin xác nhận từ Kafka (topic, partition, offset).
     * @throws KafkaException Khi bị gián đoạn, lỗi kết nối hoặc quá thời gian chờ (10s).
     */
    public PublishedOrder publishOrder(OrderCreatedEvent event) {
        try {
            // Gửi message với: Topic, Key (orderId) và Value (event)
            // .get(10, TimeUnit.SECONDS) chặn luồng để đợi Broker xác nhận (ACK) tối đa 10 giây
            var metadata = kafkaTemplate.send(KafkaConfig.TOPIC_ORDER_EVENTS, event.orderId(), event)
                    .get(10, TimeUnit.SECONDS).getRecordMetadata();

            log.info("[Producer] Gửi thành công: key={} status={} partition={} offset={}",
                    event.orderId(), event.status(), metadata.partition(), metadata.offset());

            // Trả về bản ghi chứa thông tin vị trí lưu trữ thực tế trên Kafka
            return new PublishedOrder(event.orderId(), event.status(), metadata.topic(), metadata.partition(), metadata.offset());

        } catch (InterruptedException ex) {
            // Khôi phục trạng thái ngắt luồng
            Thread.currentThread().interrupt();
            throw new KafkaException("Bị gián đoạn trong khi chờ Kafka xác nhận", ex);
        } catch (ExecutionException ex) {
            // Lỗi từ phía broker không ghi được bản ghi
            throw new KafkaException("Kafka từ chối hoặc không thể xác nhận bản ghi", ex.getCause());
        } catch (TimeoutException ex) {
            // Quá thời gian 10 giây mà chưa nhận được ACK
            throw new KafkaException("Quá thời gian (Timeout) chờ Kafka xác nhận", ex);
        }
    }

    /**
     * RECORD: PublishedOrder
     * Chứa thông tin vị trí bản ghi đã được ghi thành công trên Broker.
     */
    public record PublishedOrder(String orderId, String eventStatus, String topic, int partition, long offset) {}
}
