package me.study;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

@Slf4j
public class SimpleStreamsApplication {

    private static final String APPLICATION_NAME = "streams-application";
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String STREAM_LOG = "stream_log";
    private static final String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(STREAM_LOG).to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), configs);
        streams.start();
    }
}
