package com.example.sparkdemo.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
//单例模式， 在rddforeach中单例模式发消息 ，相当于那个executor上有这个类的jar包，自己创建了单例producer
public class KafkaSink implements Serializable {
    private  static class MyProducer{
        private static final KafkaTemplate<String,String> producer = new KafkaTemplate<String,String>(new DefaultKafkaProducerFactory<>(KafkaSink.getProducerConfig()));
    } ;


    private KafkaSink(){

    }

    public static KafkaTemplate getInstance(){
        return MyProducer.producer;
    }

    public void send(String topic, String key, String data) {
        //   log.info("sending key='{}' payload='{}' to topic='{}'",key, data, topic);
        MyProducer.producer.send(topic,key,data);
    }

    public static Map<String,Object> getProducerConfig(){

        Map<String, Object> props = new HashMap<>();
        // list of host:port pairs used for establishing the initial connections to the Kafka cluster
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "106.14.212.123:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);


        return props;
    }

}
