package me.study.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@Slf4j
@SpringBootApplication
public class SpringConsumerApplication {

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    @KafkaListener(topics = "test", groupId = "test-group-00")
    public void recordListener(ConsumerRecord<String, String> record) {
        log.info("recordListener : {}", record);
    }

    @KafkaListener(topics = "test", groupId = "test-group-01")
    public void singleTopicListener(String messageValue) {
        log.info("singleTopicListener : {}", messageValue);
    }

    @KafkaListener(topics = "test", groupId = "test-group-02",
            properties = {
                    "max.poll.interval.ms:6000",
                    "auto.offset.reset:earliest"
            })
    public void singleTopicWithPropertiesListener(String messageValue) {
        log.info("singleTopicWithPropertiesListener : {}", messageValue);
    }

    @KafkaListener(topics = "test", groupId = "test-group-03", concurrency = "3")
    public void concurrentTopicListener(String messageValue) {
        log.info("concurrentTopicListener : {}", messageValue);
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "test01", partitions = {"0", "1"}),
            @TopicPartition(topic = "test02", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3"))
    }, groupId = "test-group-04")
    public void listenSpecificPartition(ConsumerRecord<String, String> record) {
        log.info("listenSpecificPartition : {}", record);
    }
}
