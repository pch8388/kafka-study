package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class CustomProducer {

    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        Properties configs = PropertiesFactory.getProperties();
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "SC", "10");
        producer.send(record);
        log.info("{}", record);
        producer.flush();
        producer.close();
    }
}
