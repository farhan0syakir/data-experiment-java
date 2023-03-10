package com.example.demo;

import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
public class WordCountRestService {

	private final StreamsBuilderFactoryBean factoryBean;

	private final KafkaProducer kafkaProducer;

	@GetMapping("/count/{word}")
	public Long getWordCount(@PathVariable String word) {
		KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
		ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
				.store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
		return counts.get(word);
	}

	@GetMapping("/message/{content}")
	public void addMessage(@PathVariable String content) {
		kafkaProducer.sendMessage(content);
	}
}