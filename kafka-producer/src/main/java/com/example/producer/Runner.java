package com.example.producer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor

public class Runner implements CommandLineRunner {
    private final KafkaProducer kafkaProducer;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 100; i++) {
            // Your code goes here
            kafkaProducer.sendMessage("test-" + i);
            Thread.sleep(1000);

        }
    }
}
