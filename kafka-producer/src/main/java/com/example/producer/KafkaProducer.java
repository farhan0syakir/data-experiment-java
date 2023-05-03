package com.example.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Properties;

@Slf4j
@AllArgsConstructor
@Component
public class KafkaProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String message) {
		String inputTopic = "my-input-topic";
		kafkaTemplate.send(inputTopic, message)
				.addCallback(
						result -> log.info("Message sent to topic: {}", message),
						ex -> log.error("Failed to send message", ex)
				);
	}
}
