package com.example.sparkdemo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 包装类 ,专门用来给spark 广播 ，在rddforeach中单例模式发消息
 */
public class KafkaProducerClient implements Serializable {

    /**
     * 必须序列化，不序列化无法在executor端使用
     *
     */

    private KafkaTemplate<String, String> kafkaProducer = null;
  //  private  Properties kafkaProduceProperties;


    public KafkaProducerClient() {
      //  kafkaProduceProperties = properties;
    }

//    public Properties getProperties(){
//        return kafkaProduceProperties;
//    }
    // 由于Producer没有实现Serialiable接口，只能采用懒加载的方式
    public synchronized KafkaTemplate<String, String> getProducer() {
        if (kafkaProducer!=null) {

            return kafkaProducer;
        }
        kafkaProducer = new KafkaTemplate<String,String>(new DefaultKafkaProducerFactory<>(KafkaProducerClient.getProducerConfig()));

        return kafkaProducer;
    }

    public void send(String topic,String key,String value){
        getProducer().send(new ProducerRecord<String,String>(topic,key,value));
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
