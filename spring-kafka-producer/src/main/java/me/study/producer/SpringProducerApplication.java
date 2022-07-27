package me.study.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static final String TOPIC_NAME = "test";

    private final KafkaTemplate<String, String> template;

    public SpringProducerApplication(@Qualifier("customKafkaTemplate") KafkaTemplate<String, String> template) {
        this.template = template;
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) throws InterruptedException {
//        for (int i = 0; i < 10; i++) {
//            template.send(TOPIC_NAME, "test" + i);
//        }
//        System.exit(0);

        ListenableFuture<SendResult<String, String>> future = template.send(TOPIC_NAME, "test-custom-01");
        future.addCallback(new KafkaSendCallback<>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("success");
            }

            @Override
            public void onFailure(KafkaProducerException ex) {
                log.info("fail");
            }
        });

        Thread.sleep(1000);
    }
}
