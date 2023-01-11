package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
@Component
public class KafkaStreamRunner implements CommandLineRunner {
	@Override
	public void run(String... args) throws Exception {
		log.info("stream runner");
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-example");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream("input-topic");
		KTable<String, Long> wordCounts = inputStream
				.mapValues(value -> value.toLowerCase())
				.flatMapValues(value -> Arrays.asList(value.split(" ")))
				.groupBy((key, value) -> value)
				.count(Materialized.as("word-counts-store"));

		wordCounts.toStream().to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams streams = new KafkaStreams(builder.build(), config);
		streams.start();
	}
}
