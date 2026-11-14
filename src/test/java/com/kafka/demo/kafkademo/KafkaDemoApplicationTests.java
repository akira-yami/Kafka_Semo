package com.kafka.demo.kafkademo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.demo.kafkademo.controller.OrderController;
import com.kafka.demo.kafkademo.producer.OrderProducer;
import com.kafka.demo.kafkademo.producer.OrderProducer.PublishedOrder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.kafka.demo.kafkademo.config.KafkaConfig.TOPIC_ORDER_EVENTS;
import static com.kafka.demo.kafkademo.config.KafkaConfig.TOPIC_ORDER_EVENTS_DLT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * ============================================================================
 * TEST SUITE: KafkaDemoApplicationTests
 * VAI TRÒ: Kiểm thử tích hợp toàn diện (Integration Tests) cho hệ thống Kafka Demo.
 * 
 * ĐẶC ĐIỂM:
 * - Sử dụng @EmbeddedKafka để tạo cụm Kafka Broker giả lập ngay trong bộ nhớ (In-memory),
 *   giúp chạy kiểm thử tự động độc lập mà KHÔNG cần Docker ngoài.
 * - Kiểm tra tất cả 4 kịch bản: Normal, Same-Key, Batch, và Error Retry/DLT.
 * ============================================================================
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        brokerProperties = {"auto.create.topics.enable=false", "group.initial.rebalance.delay.ms=0"})
class KafkaDemoApplicationTests {

    private static final List<String> ORDER_GROUPS = List.of("email-group", "inventory-group", "billing-group");
    private static final TopicPartition DLT_PARTITION = new TopicPartition(TOPIC_ORDER_EVENTS_DLT, 0);

    private final ObjectMapper json = new ObjectMapper();

    @Autowired
    private MockMvc mvc;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaTemplate<Object, Object> template;

    /**
     * TEST CASE 1: Kiểm tra luồng đẩy tin, xử lý song song và khả năng phục hồi khi gặp bản ghi lỗi.
     * Bao gồm:
     * - Kiểm tra validate tham số (amount âm trả 400 Bad Request).
     * - Kịch bản Normal: Fan-out đến cả 3 nhóm và commit offset thành công.
     * - Kịch bản Same-Key: Cùng partition, giữ nguyên thứ tự offset tăng dần.
     * - Kịch bản Batch: Phân bổ dữ liệu đều qua cả 3 partitions (0, 1, 2).
     * - Kịch bản Poison Pill: Đơn hàng lỗi hoặc JSON sai định dạng được đẩy an toàn sang DLT.
     */
    @Test
    void demoPublishesAndCommitsEvenWhenARecordIsInvalid() throws Exception {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));
             var reader = new KafkaConsumer<>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()),
                     new StringDeserializer(), new ByteArrayDeserializer())) {
            
            // 1. Kiểm tra validation phía client: Số tiền âm phải bị từ chối với HTTP 400
            mvc.perform(post("/api/orders").param("amount", "-1")).andExpect(status().isBadRequest());

            // 2. Kiểm tra kịch bản Normal: Cả 3 consumer groups đều commit offset thành công
            JsonNode normal = publish("/api/orders").required("records").get(0);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                for (String group : ORDER_GROUPS) {
                    assertCommitted(admin, group, sourcePartition(normal), normal.required("offset").asLong() + 1);
                }
            });

            // 3. Kiểm tra kịch bản Same-Key: 3 sự kiện cùng key phải chung partition và offset tăng dần
            JsonNode sameKey = publish("/api/orders/same-key").required("records");
            assertThat(sameKey).hasSize(3);
            assertThat(sameKey).extracting(record -> record.required("eventStatus").asText())
                    .containsExactly("CREATED", "PAID", "DELIVERED");
            TopicPartition orderedPartition = sourcePartition(sameKey.get(0));
            assertThat(sameKey).allMatch(record -> sourcePartition(record).equals(orderedPartition));
            assertThat(sameKey).extracting(record -> record.required("offset").asLong())
                    .doesNotHaveDuplicates().isSorted();

            // 4. Kiểm tra kịch bản Batch: 10 tin nhắn được phân bổ vào cả 3 partitions (0, 1, 2)
            JsonNode batch = publish("/api/orders/batch").required("records");
            assertThat(batch).hasSize(10);
            assertThat(batch).extracting(record -> record.required("partition").asInt())
                    .contains(0, 1, 2);

            // 5. Kiểm tra kịch bản Poison Pill (Số tiền âm): Billing commit offset sau khi chuyển DLT
            JsonNode invalid = publish("/api/orders/error").required("records").get(0);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertCommitted(admin, "billing-group", sourcePartition(invalid),
                            invalid.required("offset").asLong() + 1));

            // 6. Kiểm tra Poison Pill dạng JSON hỏng: Ứng dụng không bị sập và bản ghi sau đó vẫn đọc được
            byte[] malformed = "malformed json".getBytes(StandardCharsets.UTF_8);
            var badRecord = template.send(TOPIC_ORDER_EVENTS, "after-poison", malformed)
                    .get(10, TimeUnit.SECONDS).getRecordMetadata();
            JsonNode afterPoison = publish("/api/orders?orderId=after-poison").required("records").get(0);
            assertThat(afterPoison.required("partition").asInt()).isEqualTo(badRecord.partition());
            assertThat(afterPoison.required("offset").asLong()).isEqualTo(badRecord.offset() + 1);

            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                for (String group : ORDER_GROUPS) {
                    assertCommitted(admin, group, sourcePartition(afterPoison), badRecord.offset() + 2);
                }
                assertCommitted(admin, "dlt-monitor-group", DLT_PARTITION, 4);
            });

            // 7. Xác nhận các bản ghi lỗi đã nằm trong Dead Letter Topic kèm header chỉ định nhóm lỗi
            reader.assign(Set.of(DLT_PARTITION));
            reader.seekToBeginning(Set.of(DLT_PARTITION));
            var deadLetters = StreamSupport.stream(reader.poll(Duration.ofSeconds(5)).spliterator(), false).toList();
            assertThat(deadLetters).hasSize(4);
            assertThat(deadLetters.getFirst().key()).isEqualTo(invalid.required("orderId").asText());
            assertThat(deadLetters.subList(1, 4)).allSatisfy(record -> {
                assertThat(record.key()).isEqualTo("after-poison");
                assertThat(record.value()).isEqualTo(malformed);
            }).extracting(record -> new String(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)
                    .value(), StandardCharsets.UTF_8)).containsExactlyInAnyOrderElementsOf(ORDER_GROUPS);
        }
    }

    /**
     * TEST CASE 2: Kiểm tra xử lý lỗi khi gửi tin nhắn bị đứt quãng giữa chừng.
     * Khi Broker bị lỗi hoặc không xác nhận ở sự kiện thứ hai, API phải trả về HTTP 503
     * và chỉ liệt kê các bản ghi đã được xác nhận thành công trước đó.
     */
    @Test
    void reportsOnlyConfirmedRecordsWhenPublishingStopsMidRequest() {
        OrderProducer producer = mock(OrderProducer.class);
        var first = new PublishedOrder("test", "CREATED", TOPIC_ORDER_EVENTS, 1, 42);
        when(producer.publishOrder(any())).thenReturn(first).thenThrow(new KafkaException("No confirmation"));

        var response = new OrderController(producer).createSameKeyOrders("test");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsEntry("status", "PARTIALLY_PUBLISHED")
                .containsEntry("records", List.of(first))
                .containsEntry("unconfirmedRecord", Map.of("orderId", "test", "eventStatus", "PAID"));
        verify(producer, times(2)).publishOrder(any());
    }

    // Helper: Gửi HTTP POST và parse phản hồi JSON
    private JsonNode publish(String path) throws Exception {
        return json.readTree(mvc.perform(post(path)).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsByteArray());
    }

    // Helper: Trích xuất TopicPartition từ JsonNode kết quả
    private TopicPartition sourcePartition(JsonNode record) {
        return new TopicPartition(record.required("topic").asText(), record.required("partition").asInt());
    }

    // Helper: Kiểm tra offset của Consumer Group đã được commit đạt mức kỳ vọng
    private void assertCommitted(Admin admin, String group, TopicPartition partition, long expected) throws Exception {
        var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                .get(3, TimeUnit.SECONDS).get(partition);
        assertThat(offset).as("%s offset for %s", group, partition).isNotNull();
        assertThat(offset.offset()).isGreaterThanOrEqualTo(expected);
    }
}
