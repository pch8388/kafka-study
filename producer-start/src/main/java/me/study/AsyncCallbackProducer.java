package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class AsyncCallbackProducer {

    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PropertiesFactory.getProperties());

        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        log.info("{}", record);

        producer.send(record, (metadata, e) -> {
            if (e != null) {
                log.error(e.getMessage(), e);
            } else {
                log.info(metadata.toString());
            }
        });

        producer.flush();
        producer.close();
    }
}
