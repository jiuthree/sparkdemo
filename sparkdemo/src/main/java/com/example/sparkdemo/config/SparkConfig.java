package com.example.sparkdemo.config;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setAppName("SparkStreaming")
                //本地起4个worker线程在本地运行spark程序
                .setMaster("local[4]")
                .set("spark.executor.memory","2g");
    }
}
