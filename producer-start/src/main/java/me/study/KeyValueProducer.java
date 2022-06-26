package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KeyValueProducer {

    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PropertiesFactory.getProperties());

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "SC", "10");
        producer.send(record);
        log.info("{}", record);
        producer.flush();
        producer.close();
    }
}
