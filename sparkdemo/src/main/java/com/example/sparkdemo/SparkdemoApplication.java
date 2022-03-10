package com.example.sparkdemo;

import com.example.sparkdemo.kafka.SparkConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkdemoApplication implements CommandLineRunner {

    private final SparkConsumerService sparkConsumerService;

    @Autowired
    public SparkdemoApplication(SparkConsumerService sparkConsumerService) {
        this.sparkConsumerService = sparkConsumerService;
    }

    public static void main(String[] args) {
        SpringApplication.run(SparkdemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        sparkConsumerService.run();
    }
}
