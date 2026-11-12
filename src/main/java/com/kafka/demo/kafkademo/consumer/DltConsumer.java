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

@Service
public class DltConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltConsumer.class);

    @KafkaListener(
            topics = KafkaConfig.TOPIC_ORDER_EVENTS_DLT,
            groupId = "dlt-monitor-group",
            concurrency = "1",
            properties = "value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    public void onDltMessage(ConsumerRecord<String, byte[]> record) {
        var originalPartition = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION);
        var originalOffset = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET);
        log.warn("[DLT] key={} sourceGroup={} sourceTopic={} sourcePartition={} sourceOffset={} dltPartition={} dltOffset={} reason={}",
                record.key(), textHeader(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                textHeader(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                originalPartition != null && originalPartition.value() != null && originalPartition.value().length == Integer.BYTES
                        ? ByteBuffer.wrap(originalPartition.value()).getInt() : null,
                originalOffset != null && originalOffset.value() != null && originalOffset.value().length == Long.BYTES
                        ? ByteBuffer.wrap(originalOffset.value()).getLong() : null,
                record.partition(), record.offset(), textHeader(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
    }

    private String textHeader(ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
