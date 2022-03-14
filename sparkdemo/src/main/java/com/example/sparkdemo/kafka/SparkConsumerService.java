package com.example.sparkdemo.kafka;

//import com.example.sparkdemo.config.KafkaConsumerConfig;
import com.example.sparkdemo.entity.TDigestDataStruct;
import com.example.sparkdemo.util.HashTagsUtils;
import com.example.sparkdemo.util.MyRedisUtils;
import com.example.sparkdemo.util.TDigestUtils;
import com.example.sparkdemo.vo.MessageDataInfoVo;
import com.google.gson.Gson;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;


@Service
public class SparkConsumerService {
    private final Logger log = LoggerFactory.getLogger(SparkConsumerService.class);
    private final SparkConf sparkConf;

    public static ConcurrentHashMap<String, AVLTreeDigest> AVLTreeDigestMap = new ConcurrentHashMap<>();
    //public static AVLTreeDigest avlTreeDigest = new AVLTreeDigest(100);

    //   private final Collection<String> topics;
 //   private final KafkaConsumerConfig kafkaConsumerConfig;
    //这个虽然没有用到这个变量，但是一样要注入，需要它来获取topics
    //private final KafkaProperties properties;

    private final org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;




    @Autowired
    public SparkConsumerService(SparkConf sparkConf, KafkaProperties kafkaProperties) {
        this.sparkConf = sparkConf;
    //    this.kafkaConsumerConfig = kafkaConsumerConfig;

        //这一步来注入topics
       // this.topics = Arrays.asList(kafkaProperties.getTemplate().getDefaultTopic());
        this.kafkaProperties = kafkaProperties;
    }

    public void run() {
        log.debug("Running Spark Consumer Service..");

        // Create context with a 0.2 seconds batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(200));


        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singleton(kafkaProperties.getTemplate().getDefaultTopic()), kafkaProperties.buildConsumerProperties()));



        // Get the lines, split them into words, count the words and print
        //JavaDStream<String> lines = messages.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());


        JavaPairDStream<String, Tuple2<String, String>> datas = messages.mapToPair(stringStringConsumerRecord -> {
            //     System.out.println(stringStringConsumerRecord.value());
            MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
            return new Tuple2<>(stringStringConsumerRecord.key(), new Tuple2<>(stringStringConsumerRecord.key(), messageDataInfo.getValue()));
        });


        //第一个元素是key，就是按照这个来分组计算的
        JavaPairDStream<String, Tuple2<String, String>> stream = datas.reduceByKey(
                (str1, str2) -> {
                    double v = Double.parseDouble(str1._2());
              //      System.out.println(v);

                    // 这一步的逻辑要注意，应该是每一个分组用一个avlDigest
                    AVLTreeDigest tDigest = AVLTreeDigestMap.getOrDefault(str1._1(), new AVLTreeDigest(100));
                    //avlTreeDigest.add(v);
                    tDigest.add(v);
                    //每次修改完要记得put
                    AVLTreeDigestMap.put(str1._1(),tDigest);
                    //最后一个元素没有加上
                    return str2;
                }
        );

        stream.map((tuple2)->{
        //先把最后一个元素给加上
            double v = Double.parseDouble(tuple2._2()._2());
       //     System.out.println(v);
         //   avlTreeDigest.add(v);
            AVLTreeDigest tDigest = AVLTreeDigestMap.getOrDefault(tuple2._2()._1(), new AVLTreeDigest(100));
            //avlTreeDigest.add(v);
            tDigest.add(v);


            TDigest redisDigest = MyRedisUtils.aggregateAndSaveToRedis(tDigest, tuple2._1);

            //要确保这个方法，线程安全
            //avlTreeDigest = new AVLTreeDigest(100);
            tDigest = new AVLTreeDigest(100);
            AVLTreeDigestMap.put(tuple2._2()._1(),tDigest);
            System.out.println("update!!!");
            System.out.println("size:"+redisDigest.size());
            System.out.println(TDigestUtils.getQuantile(redisDigest, 0.9));
            return null;
        }).print();


//        datas.map(str->{
//            double v = Double.parseDouble(str._2());
//            System.out.println(v);
//            //现在这个代码的问题在于完全是串行执行的
//            AVLTreeDigest avlTreeDigest = new AVLTreeDigest(100);
//            avlTreeDigest.add(v);
//
//            TDigest redisDigest = MyRedisUtils.aggregateAndSaveToRedis(avlTreeDigest, "key");
//            //判断这个是不是一直在更新
//            System.out.println("update!!!");
//            System.out.println(redisDigest.size());
//
//            //转换后想办法算出那个百分数
//
//            System.out.println(TDigestUtils.getQuantile(redisDigest, 0.9));
//
//            return v;
//        }).print();





//        //Count the tweets and print
//        lines.count()
//                .map(cnt -> "Popular hash tags in last 60 seconds (" + cnt + " total tweets):")
//                .print();
//
//        lines.print();
//
//        //
//        lines.flatMap(text -> HashTagsUtils.hashTagsFromTweet(text))
//                .mapToPair(hashTag -> new Tuple2<>(hashTag, 1))
//                .reduceByKey((a, b) -> Integer.sum(a, b))
//                .mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap())
//                .foreachRDD(rrdd -> {
//                    log.info("---------------------------------------------------------------");
//                    //Counts
//                    rrdd.sortByKey(false).collect()
//                            .forEach(record -> {
//                                log.info(String.format(" %s (%d)", record._2, record._1));
//                            });
//                });

        // Start the computation
        jssc.start();


        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            log.error("Interrupted: {}", e);
            // Restore interrupted state...
        }
    }
}
