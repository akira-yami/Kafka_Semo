package com.kafka.demo.kafkademo.consumer;

import com.kafka.demo.kafkademo.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

@Service
public class DltConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltConsumer.class);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS_DLT,
            groupId = "dlt-monitor-group"
    )
    public void onDltMessage(
            ConsumerRecord<String, Object> record,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] origTopic,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] origOffset,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) byte[] exceptionMsg
    ) {
        String reason = exceptionMsg != null ? new String(exceptionMsg, StandardCharsets.UTF_8) : "Unknown";
        String topic = origTopic != null ? new String(origTopic, StandardCharsets.UTF_8) : "N/A";

        log.warn("================================================================================");
        log.warn("[DEAD LETTER TOPIC MONITOR] PHÁT HIỆN TIN NHẮN BỊ CÔ LẬP TRONG DLT!");
        log.warn("Key: {}, Value: {}", record.key(), record.value());
        log.warn("Original Topic: {}, Partition: {}, Offset trong DLT: {}", topic, record.partition(), record.offset());
        log.warn("Lý do thất bại (Exception Message): {}", reason);
        log.warn("================================================================================");
    }
}
