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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@Service
public class KafkaProducerService {

    public static List<List<Integer>> deviceConfigs = new ArrayList<>();
    public static List<Integer> deviceRanges = new ArrayList<>();


    private final Logger log = LoggerFactory.getLogger(getClass());

    private final KafkaProperties kafkaProperties;


    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaProperties kafkaProperties, KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaTemplate = kafkaTemplate;
    }

    //probability传一个1~10的数字，表示概率，传5表示有一半的概率触发
    public static boolean isTriggered(int probability) {

        Random random = new Random();
        int num = random.nextInt(10) + 1;  //原本是左闭右开 ，+1之后就是 1到10
        return num <= probability;

    }

    //probability 同上， interval 代表状态切换的最小时间
    public static int getRandomValueOfDevice(int probability, int interval, List<Integer> thresholds, int range, int current, int lastValue) {
        Random random = new Random();
        int num = random.nextInt(range * 2) - range;

        int threshold = thresholds.get(getMinDistanceIndex(thresholds, lastValue));

        current++;
        if (current > interval) {
            //自己在函数外部维护这个current，这里就不返回了
            current -= interval;

            if (isTriggered(probability)) {
                int index = new Random().nextInt(thresholds.size());
                return thresholds.get(index) + num >= 0 ? thresholds.get(index) + num : 0;
            } else {
                return threshold + num >= 0 ? threshold + num : 0;
            }

        } else {

            return threshold + num >= 0 ? threshold + num : 0;
        }
    }

    public static int getMinDistanceIndex(List<Integer> list, int target) {
        int minIndex = 0;
        for (int i = 0; i < list.size() - 1; i++) {
            if (Math.abs(list.get(minIndex) - target) > Math.abs(list.get(i + 1) - target)) {
                minIndex = i + 1;
            }
        }
        return minIndex;
    }

    public void send(String topic, String payload) {
        log.info("sending payload='{}' to topic='{}'", payload, topic);
        kafkaTemplate.send(topic, payload);
    }

    public void send(String topic, String key, String data) {
        //   log.info("sending key='{}' payload='{}' to topic='{}'",key, data, topic);
        kafkaTemplate.send(topic, key, data);
    }

    public void run() {
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
        ArrayList<Integer> deviceConfig1 = new ArrayList<>(Arrays.asList(0, 30));
        ArrayList<Integer> deviceConfig2 = new ArrayList<>(Arrays.asList(0, 50));
        ArrayList<Integer> deviceConfig3 = new ArrayList<>(Arrays.asList(0, 100));
        ArrayList<Integer> deviceConfig4 = new ArrayList<>(Arrays.asList(0, 1750));
        ArrayList<Integer> deviceConfig5 = new ArrayList<>(Arrays.asList(0, 200, 1000)); //冰箱  或者洗衣机（满载时达到最大功率）
        ArrayList<Integer> deviceConfig6 = new ArrayList<>(Arrays.asList(0, 30));
        ArrayList<Integer> deviceConfig7 = new ArrayList<>(Arrays.asList(0, 500, 1500)); //空调
        ArrayList<Integer> deviceConfig8 = new ArrayList<>(Arrays.asList(0, 500)); //电饭煲
        ArrayList<Integer> deviceConfig9 = new ArrayList<>(Arrays.asList(0, 3300)); //电烤箱
        ArrayList<Integer> deviceConfig10 = new ArrayList<>(Arrays.asList(0, 800)); //吸尘器
        deviceConfigs.addAll(Arrays.asList(deviceConfig1, deviceConfig2, deviceConfig3, deviceConfig4, deviceConfig5, deviceConfig6, deviceConfig7, deviceConfig8, deviceConfig9, deviceConfig10));

        deviceRanges.addAll(Arrays.asList(5, 8, 10, 100, 20, 4, 50, 50, 300, 80));


        produceData();
    }

    private String RandomWords() {//产生随机单词
        String[] s = {"good", "cheer", "strive", "optimistic", "hello", "word", "tercher", "student",
                "book", "genius", "handsome", "beautiful", "health", "happy", "exercice", "computer",
                "english", "jspanese", "eat", "me", "reset", "center", "blue", "green", "yellow"};
        Random random = new Random();
        int num = random.nextInt(10);
        String res = "";
        for (int i = 0; i < num; i++) {
            int b = random.nextInt(21);//定义随机数区间[0,20]
            res += " " + s[b];
        }
        return res;
    }

    private void test1() {
        Thread thread = new Thread(() -> {
            int i = 0;
            Gson gson = new Gson();
            while (true) {

                for (int q = 0; q < 100000; i++, q++) {
                    MessageDataInfoVo mes = new MessageDataInfoVo();
                    mes.setValue(String.valueOf(i));
                    send(kafkaProperties.getTemplate().getDefaultTopic(), "key", gson.toJson(mes));
                }
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


    //编写几个辅助随机数生成的函数

    private void test2() {
        for (int num = 1; num <= 5; num++) {
            int finalNum = num;
            Thread thread = new Thread(() -> {
                int i = 0;
                Gson gson = new Gson();
                while (true) {

                    for (int q = 0; q < 10; i++, q++) {
                        MessageDataInfoVo mes = new MessageDataInfoVo();
                        mes.setValue(String.valueOf(10));
                        send(kafkaProperties.getTemplate().getDefaultTopic(), "key" + finalNum, gson.toJson(mes));
                    }
                    //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
                    try {
                        Thread.sleep(100l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "producer-thread" + finalNum);
            thread.setDaemon(true);
            thread.start();
        }
    }

    private void test3() {
        for (int num = 1; num <= 5; num++) {
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
                        keyInfoVo.setKey("key" + finalNum);
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
            }, "producer-thread" + finalNum);
            thread.setDaemon(true);
            thread.start();
        }
    }

    //写一个复杂点的模拟函数，去生成真正给实验用的数据   10个设备，两组数据，一组电量 一组功率
    private void produceData() {
        for (int num = 1; num <= 10; num++) {
            int finalNum = num;
            Thread thread = new Thread(() -> {
                int current = 0;
                int lastValue = deviceConfigs.get(finalNum - 1).get(1);
                int interval = 5 * 60;
                Gson gson = new Gson();
                while (true) {


                    MessageDataInfoVo powerMes = new MessageDataInfoVo();


                    current++;
                    lastValue = getRandomValueOfDevice(5, interval, deviceConfigs.get(finalNum - 1), deviceRanges.get(finalNum - 1), current, lastValue);
                    if (current > interval) {
                        current -= interval;
                    }

                    powerMes.setValue(String.valueOf(lastValue));


                    powerMes.setName("device" + finalNum);
                    KeyInfoVo powerKeyInfoVo = new KeyInfoVo();
                    powerKeyInfoVo.setKey("power_p");
                    GroupInfo powerGroupInfo = new GroupInfo();
//                        GroupInfoMetadata metadata1 = new GroupInfoMetadata();
//                        metadata1.setValue("MapState");
//                        groupInfo.addMetaData(metadata1);
//                        GroupInfoMetadata metadata2 = new GroupInfoMetadata();
//                        metadata2.setValue("WindowState");
//                        groupInfo.addMetaData(metadata2);
                    GroupInfoMetadata metadata3 = new GroupInfoMetadata();
                    metadata3.setValue("Quantile");
                    powerGroupInfo.addMetaData(metadata3);
                    GroupInfoMetadata metadata4 = new GroupInfoMetadata();
                    metadata4.setValue("AnalysisState");
                    powerGroupInfo.addMetaData(metadata4);
                    powerKeyInfoVo.setGroupInfo(powerGroupInfo.toGroupInfoVo());
                    powerMes.setKeyInfo(powerKeyInfoVo);

                    send(kafkaProperties.getTemplate().getDefaultTopic(), "key", gson.toJson(powerMes));

                    MessageDataInfoVo energyMes = new MessageDataInfoVo();

                    //换算为度数
                    double electricity = lastValue * 1.0 / 3600 / 1000;
                    energyMes.setValue(String.valueOf(electricity));


                    energyMes.setName("device" + finalNum);
                    KeyInfoVo energyKeyInfoVo = new KeyInfoVo();
                    energyKeyInfoVo.setKey("energy_pt");
                    GroupInfo energyGroupInfo = new GroupInfo();
                    GroupInfoMetadata metadata1 = new GroupInfoMetadata();
                    metadata1.setValue("MapState");
                    energyGroupInfo.addMetaData(metadata1);
                    GroupInfoMetadata metadata2 = new GroupInfoMetadata();
                    metadata2.setValue("WindowState");
                    energyGroupInfo.addMetaData(metadata2);
//                        GroupInfoMetadata metadata3 = new GroupInfoMetadata();
//                        metadata3.setValue("Quantile");
//                        groupInfo.addMetaData(metadata3);
                    energyKeyInfoVo.setGroupInfo(energyGroupInfo.toGroupInfoVo());
                    energyMes.setKeyInfo(energyKeyInfoVo);

                    send(kafkaProperties.getTemplate().getDefaultTopic(), "key", gson.toJson(energyMes));


                    //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
                    try {
                        Thread.sleep(1000l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }, "producer-thread" + finalNum);
            thread.setDaemon(true);
            thread.start();
        }

//        for (int num = 1; num <= 10; num++) {
//            int finalNum = num;
//            Thread thread = new Thread(() -> {
//                int i = 0;
//                Gson gson = new Gson();
//                while (true) {
//
//                    for (int q = 0; q < 5; i++, q++) {
//                        MessageDataInfoVo mes = new MessageDataInfoVo();
//
//                        mes.setValue(String.valueOf(q));
//
//
//                        mes.setName("device" + finalNum);
//                        KeyInfoVo keyInfoVo = new KeyInfoVo();
//                        keyInfoVo.setKey("energy_pt");
//                        GroupInfo groupInfo = new GroupInfo();
//                        GroupInfoMetadata metadata1 = new GroupInfoMetadata();
//                        metadata1.setValue("MapState");
//                        groupInfo.addMetaData(metadata1);
//                        GroupInfoMetadata metadata2 = new GroupInfoMetadata();
//                        metadata2.setValue("WindowState");
//                        groupInfo.addMetaData(metadata2);
////                        GroupInfoMetadata metadata3 = new GroupInfoMetadata();
////                        metadata3.setValue("Quantile");
////                        groupInfo.addMetaData(metadata3);
//                        keyInfoVo.setGroupInfo(groupInfo.toGroupInfoVo());
//                        mes.setKeyInfo(keyInfoVo);
//
//                        send(kafkaProperties.getTemplate().getDefaultTopic(), "key", gson.toJson(mes));
//                    }
//                    //send(kafkaProperties.getTemplate().getDefaultTopic(),RandomWords());
//                    try {
//                        Thread.sleep(1000l);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }, "producer-thread" + finalNum);
//            thread.setDaemon(true);
//            thread.start();
//        }


    }


}