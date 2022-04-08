package com.example.sparkdemo;

import com.example.sparkdemo.dao.ElectricityPB2EDao;
import com.example.sparkdemo.dao.ElectricityPHPEDao;
import com.example.sparkdemo.dao.MyCreateTableTestEntityDao;
import com.example.sparkdemo.entity.ElectricityPB2E;
import com.example.sparkdemo.entity.ElectricityPHPE;
import com.example.sparkdemo.entity.TDigestDataStruct;
import com.example.sparkdemo.kafka.KafkaProducerService;
import com.example.sparkdemo.kafka.SparkConsumerService;
import com.example.sparkdemo.util.CsvUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
public class SparkdemoApplication implements CommandLineRunner {

    @Resource
    ElectricityPB2EDao electricityPB2EDao;

    @Resource
    ElectricityPHPEDao electricityPHPEDao;

    private final SparkConsumerService sparkConsumerService;
    private final KafkaProducerService kafkaProducerService;

    //暂时用它来代替redis
    public static ConcurrentHashMap<String, TDigestDataStruct> TDigestDataMap = new ConcurrentHashMap<>();


    @Autowired
    public SparkdemoApplication(SparkConsumerService sparkConsumerService, KafkaProducerService kafkaProducerService) {
        this.sparkConsumerService = sparkConsumerService;
        this.kafkaProducerService = kafkaProducerService;

    }

    public static void main(String[] args) {
        SpringApplication.run(SparkdemoApplication.class, args);
       // SpringApplication.run(SparkdemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

   //     kafkaProducerService.run();
   //     sparkConsumerService.run();


    }

}


