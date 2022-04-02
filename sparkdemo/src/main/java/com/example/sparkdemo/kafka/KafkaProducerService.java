package com.example.sparkdemo.kafka;

import com.example.sparkdemo.vo.GroupInfo;
import com.example.sparkdemo.vo.GroupInfoMetadata;
import com.example.sparkdemo.vo.KeyInfoVo;
import com.example.sparkdemo.vo.MessageDataInfoVo;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Random;

@Service
public class KafkaProducerService  {

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

    public void send(String topic, String key, String data) {
     //   log.info("sending key='{}' payload='{}' to topic='{}'",key, data, topic);
        kafkaTemplate.send(topic,key,data);
    }

    public void run(){
//        Thread thread = new Thread(() -> {
//            int i = 0;
//            Gson gson = new Gson();
//            while (true){
//
//                for(int q=0;q<100000;i++,q++){
//                MessageDataInfoVo mes = new MessageDataInfoVo();
//                mes.setValue(String.valueOf(i));
//                send(kafkaProperties.getTemplate().getDefaultTopic(),"key",gson.toJson(mes));}
//                //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
//                try {
//                    Thread.sleep(100l);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }, "producer-thread");
//        thread.setDaemon(true);
//        thread.start();
        test3();
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

    private void test1(){
        Thread thread = new Thread(() -> {
            int i = 0;
            Gson gson = new Gson();
            while (true){

                for(int q=0;q<100000;i++,q++){
                    MessageDataInfoVo mes = new MessageDataInfoVo();
                    mes.setValue(String.valueOf(i));
                    send(kafkaProperties.getTemplate().getDefaultTopic(),"key",gson.toJson(mes));}
                //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
                try {
                    Thread.sleep(100l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "producer-thread");
        thread.setDaemon(true);
        thread.start();
    }

    private void test2(){
        for(int num = 1;num<=5;num++) {
            int finalNum = num;
            Thread thread = new Thread(() -> {
                int i = 0;
                Gson gson = new Gson();
                while (true) {

                    for (int q = 0; q < 10; i++, q++) {
                        MessageDataInfoVo mes = new MessageDataInfoVo();
                        mes.setValue(String.valueOf(10));
                        send(kafkaProperties.getTemplate().getDefaultTopic(), "key"+ finalNum, gson.toJson(mes));
                    }
                    //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
                    try {
                        Thread.sleep(100l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "producer-thread"+finalNum);
            thread.setDaemon(true);
            thread.start();
        }
    }

    private void test3(){
        for(int num = 1;num<=5;num++) {
            int finalNum = num;
            Thread thread = new Thread(() -> {
                int i = 0;
                Gson gson = new Gson();
                while (true) {

                    for (int q = 0; q < 5; i++, q++) {
                        MessageDataInfoVo mes = new MessageDataInfoVo();
                        mes.setValue(String.valueOf(q));
                        mes.setName("device");
                        KeyInfoVo keyInfoVo = new KeyInfoVo();
                        keyInfoVo.setKey("key"+finalNum);
                        GroupInfo groupInfo = new GroupInfo();
                        GroupInfoMetadata metadata1 = new GroupInfoMetadata();
                        metadata1.setValue("MapState");
                        groupInfo.addMetaData(metadata1);
                        GroupInfoMetadata metadata2 = new GroupInfoMetadata();
                        metadata2.setValue("WindowState");
                        groupInfo.addMetaData(metadata2);
                        GroupInfoMetadata metadata3 = new GroupInfoMetadata();
                        metadata3.setValue("Quantile");
                        groupInfo.addMetaData(metadata3);
                        keyInfoVo.setGroupInfo(groupInfo.toGroupInfoVo());
                        mes.setKeyInfo(keyInfoVo);

                        send(kafkaProperties.getTemplate().getDefaultTopic(), "key", gson.toJson(mes));
                    }
                    //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
                    try {
                        Thread.sleep(100l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "producer-thread"+finalNum);
            thread.setDaemon(true);
            thread.start();
        }
    }


}