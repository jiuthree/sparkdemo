package com.example.sparkdemo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class KafkaProducerService {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KafkaProperties kafkaProperties;


    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaProperties kafkaProperties, KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String payload) {
        log.info("sending payload='{}' to topic='{}'", payload, topic);
        kafkaTemplate.send(topic, payload);
    }

    public void run(){
        Thread thread = new Thread(() -> {
            while (true){
                send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
                try {
                    Thread.sleep(1000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "producer-thread");
        thread.setDaemon(true);
        thread.start();

    }

    private String RandomWords() {//产生随机单词
        String[] s = {"good", "cheer", "strive", "optimistic", "hello", "word", "tercher", "student",
                "book", "genius", "handsome", "beautiful", "health", "happy", "exercice", "computer",
                "english", "jspanese", "eat", "me","reset","center","blue","green","yellow"};
        Random random = new Random();
        int num = random.nextInt(10);
        String res = "";
        for(int i=0;i<num;i++) {
            int b = random.nextInt(21);//定义随机数区间[0,20]
            res+=" "+s[b];
        }
        return res;
    }
}