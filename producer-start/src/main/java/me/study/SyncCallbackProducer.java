package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

@Slf4j
public class SyncCallbackProducer {

    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PropertiesFactory.getProperties());

        String messageValue = "testMessage";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        log.info("{}", record);

        RecordMetadata metadata = producer.send(record).get();
        log.info(metadata.toString());

        producer.flush();
        producer.close();
    }
}
