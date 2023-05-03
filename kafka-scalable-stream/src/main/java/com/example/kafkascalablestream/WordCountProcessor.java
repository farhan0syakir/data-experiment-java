package com.example.kafkascalablestream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Slf4j
@Component
public class WordCountProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Value("${myapp.input-topic}")
    private String inputTopic;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(inputTopic, Consumed.with(STRING_SERDE, STRING_SERDE))
                .mapValues(value -> value.toUpperCase())
                .peek((key, value) -> log.info("print: {} {}", key, value))
                .to("output-topic");
    }

}
