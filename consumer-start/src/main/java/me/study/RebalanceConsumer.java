package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

@Slf4j
public class RebalanceConsumer {
    private static final String TOPIC_NAME = "test";
    private static final String GROUP_ID = "test-group";
    private static final Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        Properties configs = PropertiesFactory.getProperties(GROUP_ID);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME), new RebalaceListener(consumer));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)
                );
                consumer.commitSync(currentOffset);
            }
        }
    }

    private static class RebalaceListener implements ConsumerRebalanceListener {

        private final KafkaConsumer<String, String> consumer;

        public RebalaceListener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("Partitions are revoked");
            consumer.commitSync(currentOffset);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.warn("Partitions are assigned");
        }
    }

}