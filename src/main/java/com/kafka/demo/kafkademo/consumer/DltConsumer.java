package com.kafka.demo.kafkademo.consumer;

import com.kafka.demo.kafkademo.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * ============================================================================
 * CONSUMER: DltConsumer
 * VAI TRÒ: Dịch vụ Giám sát Tin nhắn lỗi (Dead Letter Topic Monitor).
 * THUỘC GROUP: "dlt-monitor-group".
 * 
 * ĐẶC ĐIỂM KỸ THUẬT:
 * 1. Đọc mảng byte thô: Sử dụng ByteArrayDeserializer để có thể đọc được mọi message lỗi, 
 *    kể cả các message bị lỗi cú pháp JSON mà Deserializer bình thường không parse được.
 * 2. Phân tích Headers DLT: Trích xuất các header đặc biệt do Spring Kafka tự động thêm vào
 *    để xác định nguồn gốc lỗi (Topic, Partition, Offset ban đầu và Exception message).
 * ============================================================================
 */
@Service
public class DltConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltConsumer.class);

    /**
     * LẮNG NGHE SỰ KIỆN TRÊN DEAD LETTER TOPIC (order-events-dlt)
     * 
     * @param record Bản ghi thô từ DLT.
     */
    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS_DLT,
            groupId = "dlt-monitor-group",
            concurrency = "1",
            properties = "value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    public void onDltMessage(ConsumerRecord<String, byte[]> record) {
        // Trích xuất metadata của tin nhắn gốc trước khi bị chuyển sang DLT
        var originalPartition = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION);
        var originalOffset = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);

        // Ghi log cảnh báo chi tiết giúp đội ngũ vận hành dễ dàng tra cứu và xử lý thủ công
        log.warn("[DLT MONITOR] Phát hiện đơn hàng lỗi được cô lập sang DLT: " +
                "key={} sourceGroup={} sourceTopic={} sourcePartition={} sourceOffset={} dltPartition={} dltOffset={} reason={}",
                record.key(),
                textHeader(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                textHeader(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                originalPartition != null && originalPartition.value() != null && originalPartition.value().length == Integer.BYTES
                        ? ByteBuffer.wrap(originalPartition.value()).getInt() : null,
                originalOffset != null && originalOffset.value() != null && originalOffset.value().length == Long.BYTES
                        ? ByteBuffer.wrap(originalOffset.value()).getLong() : null,
                record.partition(),
                record.offset(),
                textHeader(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)
        );
    }

    /**
     * HÀM TIỆN ÍCH: Đọc giá trị header dưới dạng chuỗi UTF-8
     */
    private String textHeader(ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
