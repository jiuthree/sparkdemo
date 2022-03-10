//package com.example.sparkdemo.kafka;
//
//import com.google.common.util.concurrent.ThreadFactoryBuilder;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.Resource;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//@Component
//public class MyKafkaConsumerListener {
//    private final static Logger logger = LoggerFactory.getLogger(MyKafkaConsumerListener.class);
//    private final ExecutorService executorService = new ThreadPoolExecutor(
//            Runtime.getRuntime().availableProcessors(),
//            Runtime.getRuntime().availableProcessors() * 2, 60, TimeUnit.SECONDS,
//            new LinkedBlockingQueue<>(50000), new ThreadFactoryBuilder().setNameFormat("MyThreadPoll2-%d").build());
//
//    @Resource
//    SparkConsumerService sparkConsumerService;
//
//    @KafkaListener(topics = "device_topic", groupId = "testGroup")
//    public void onMessage(ConsumerRecord<String, String> message) {
//        try {
//            executorService.submit(() -> processMessage(message));
//        } catch (Exception e) {
//            logger.info("submit task occurs exception ", e);
//        }
//
//    }
//
//    private void processMessage(ConsumerRecord<String, String> message){
//
//    }
//}
