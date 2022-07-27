package me.study.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringProducerApplication implements CommandLineRunner {

    private static final String TOPIC_NAME = "test";

    private final KafkaTemplate<Integer, String> template;

    public SpringProducerApplication(KafkaTemplate<Integer, String> template) {
        this.template = template;
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) {
        for (int i = 0; i < 10; i++) {
            template.send(TOPIC_NAME, "test" + i);
        }
        System.exit(0);
    }
}
