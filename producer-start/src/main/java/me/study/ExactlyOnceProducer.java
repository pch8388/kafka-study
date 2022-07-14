package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static me.study.PropertiesFactory.getProperties;

@Slf4j
public class ExactlyOnceProducer {

    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        Properties props = getProperties();
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "sc-transaction-01");

        Producer<String, String> producer = new KafkaProducer<>(props);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "exactly-test");
            producer.send(record);
            producer.flush();
            log.info("Message sent successfully");
        } catch (Exception e) {
            producer.abortTransaction();
        } finally {
            producer.commitTransaction();
            producer.close();
        }
    }
}
