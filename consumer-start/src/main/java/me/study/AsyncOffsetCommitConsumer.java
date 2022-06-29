package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class AsyncOffsetCommitConsumer {
    private final static String TOPIC_NAME = "test";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = PropertiesFactory.getProperties(GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
            }
            consumer.commitAsync((offsets, e) -> {
                if (e != null) {
                    log.error("Commit failed for offsets {}", offsets, e);
                } else {
                    log.info("Commit succeeded");
                }
            });
        }
    }
}
