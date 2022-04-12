package com.example.sparkdemo.kafka;

import com.example.sparkdemo.entity.DeviceStatisticsData;
import com.example.sparkdemo.service.DeviceStatisticsDataService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
//这个类就用来处理spark最后发的kafka，把统计消息转换为Mysql表里的数据
public class MyKafkaConsumerListener {
    private final static Logger logger = LoggerFactory.getLogger(MyKafkaConsumerListener.class);
    private final ExecutorService executorService = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors() * 2, 60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(50000), new ThreadFactoryBuilder().setNameFormat("MyThreadPoll2-%d").build());


    @Resource
    DeviceStatisticsDataService deviceStatisticsDataService;

    @KafkaListener(topics = "spark_streaming_store", groupId = "testGroup")
    public void onMessage(ConsumerRecord<String, String> message) {
        try {
            executorService.submit(() -> processMessage(message));
        } catch (Exception e) {
            logger.info("submit task occurs exception ", e);
        }

    }

    private void processMessage(ConsumerRecord<String, String> message){
        String key = message.key();
        String value = message.value();

        switch (key){
            case SparkConsumerService.WindowState:{
                DeviceStatisticsData deviceStatisticsData = new Gson().fromJson(value, DeviceStatisticsData.class);
                if(deviceStatisticsData != null){
                    System.out.println(value);
                deviceStatisticsDataService.save(deviceStatisticsData);}
            }

            case SparkConsumerService.Quantile:{
                DeviceStatisticsData deviceStatisticsData = new Gson().fromJson(value, DeviceStatisticsData.class);
                if(deviceStatisticsData != null){
                    System.out.println(value);
                    deviceStatisticsDataService.save(deviceStatisticsData);}
            }

            case SparkConsumerService.MapState:{
                DeviceStatisticsData deviceStatisticsData = new Gson().fromJson(value, DeviceStatisticsData.class);
                if(deviceStatisticsData != null){
                    System.out.println(value);
                    deviceStatisticsDataService.save(deviceStatisticsData);}
            }

            case SparkConsumerService.AnalysisState:{
                DeviceStatisticsData deviceStatisticsData = new Gson().fromJson(value, DeviceStatisticsData.class);
                if(deviceStatisticsData != null){
                    System.out.println(value);
                    deviceStatisticsDataService.save(deviceStatisticsData);}

            }

            default:{}
        }
    }


}
