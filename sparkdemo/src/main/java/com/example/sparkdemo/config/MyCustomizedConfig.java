package com.example.sparkdemo.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Configuration
@Component
@ConfigurationProperties(prefix = "my.config")
@Data
public class MyCustomizedConfig {
        String test;
        String checkpointDir;
}
