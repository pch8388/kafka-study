package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

@Slf4j
public class SimpleConsumer {

    private final static String TOPIC_NAME = "test";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(PropertiesFactory.getProperties(GROUP_ID));
        consumer.subscribe(List.of(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("{}", record);
            }
        }
    }
}
