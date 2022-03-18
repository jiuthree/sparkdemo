package com.example.sparkdemo.kafka;

//import com.example.sparkdemo.config.KafkaConsumerConfig;

import com.example.sparkdemo.config.MyCustomizedConfig;
import com.example.sparkdemo.util.MyRedisUtils;
import com.example.sparkdemo.util.TDigestUtils;
import com.example.sparkdemo.vo.MessageDataInfoVo;
import com.google.gson.Gson;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.spark.api.java.Optional;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;


@Service
public class SparkConsumerService  {
    public static ConcurrentHashMap<String, AVLTreeDigest> AVLTreeDigestMap = new ConcurrentHashMap<>();
    private final Logger log = LoggerFactory.getLogger(SparkConsumerService.class);
    private final SparkConf sparkConf;
    private final MyCustomizedConfig myCustomizedConfig;
    //public static AVLTreeDigest avlTreeDigest = new AVLTreeDigest(100);

    //   private final Collection<String> topics;
    //   private final KafkaConsumerConfig kafkaConsumerConfig;
    //这个虽然没有用到这个变量，但是一样要注入，需要它来获取topics
    //private final KafkaProperties properties;
    private final org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;

    @Resource
    KafkaProducerService kafkaProducerService;

    @Autowired
    public SparkConsumerService(SparkConf sparkConf, MyCustomizedConfig myCustomizedConfig, KafkaProperties kafkaProperties) {
        this.sparkConf = sparkConf;
        this.myCustomizedConfig = myCustomizedConfig;
        //    this.kafkaConsumerConfig = kafkaConsumerConfig;

        //这一步来注入topics
        // this.topics = Arrays.asList(kafkaProperties.getTemplate().getDefaultTopic());
        this.kafkaProperties = kafkaProperties;
    }

    public void run() {
        log.debug("Running Spark Consumer Service..");

        // Create context with a 0.2 seconds batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(1000));
        jssc.checkpoint(myCustomizedConfig.getCheckpointDir());
      // jssc.sparkContext().broadcast(kafkaProducerService);
        Broadcast<String> sendTopic = jssc.sparkContext().broadcast(myCustomizedConfig.getSendTopic());

        KafkaProducerClient kpc=new KafkaProducerClient();

        Broadcast<KafkaProducerClient> kafkaProducer = jssc.sparkContext().broadcast(kpc);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singleton(kafkaProperties.getTemplate().getDefaultTopic()), kafkaProperties.buildConsumerProperties()));


        // Get the lines, split them into words, count the words and print
        //JavaDStream<String> lines = messages.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());


        //getQuantileByTDigest(messages, 0.9);
       // statisticsByHour(messages);
       // testUpdateStateByKey(messages);
        testMapWithState(messages,kafkaProducer,sendTopic);
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

    public void getQuantileByTDigest(JavaInputDStream<ConsumerRecord<String, String>> messages, double quantile) {
        JavaPairDStream<String, Tuple2<String, String>> datas = messages.mapToPair(stringStringConsumerRecord -> {
            //     System.out.println(stringStringConsumerRecord.value());
            MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
            return new Tuple2<>(stringStringConsumerRecord.key(), new Tuple2<>(stringStringConsumerRecord.key(), messageDataInfo.getValue()));
        });


        //第一个元素是key，就是按照这个来分组计算的
        JavaPairDStream<String, Tuple2<String, String>> stream =
                datas.reduceByKey(
                        (str1, str2) -> {
                            double v = Double.parseDouble(str1._2());
                            //      System.out.println(v);

                            // 这一步的逻辑要注意，应该是每一个分组用一个avlDigest
                            AVLTreeDigest tDigest = AVLTreeDigestMap.getOrDefault(str1._1(), new AVLTreeDigest(100));
                            //avlTreeDigest.add(v);
                            tDigest.add(v);
                            //每次修改完要记得put
                            AVLTreeDigestMap.put(str1._1(), tDigest);
                            //最后一个元素没有加上
                            return str2;
                        }
                );

        stream.map((tuple2) -> {
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
            AVLTreeDigestMap.put(tuple2._2()._1(), tDigest);
            //用log会报错,报的啥scala的序列化错误，不懂   好像是driver和worker节点的问题
            System.out.println((("update!!!")));
            System.out.println(("size:" + redisDigest.size()));
            System.out.println(TDigestUtils.getQuantile(redisDigest, quantile));
            return null;
        }).print();

    }

    // todo 这个任务可以参照最近一小时的电量显示这个需求，刷新是每分钟刷新
    public void statisticsByHour(JavaInputDStream<ConsumerRecord<String, String>> messages) {
            JavaPairDStream<String, String> datas = messages.mapToPair(stringStringConsumerRecord -> {
                //     System.out.println(stringStringConsumerRecord.value());
                MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
                return new Tuple2<>(stringStringConsumerRecord.key(), messageDataInfo.getValue());
            });

            //统计最近一分钟的总量，每秒进行更新
            datas.reduceByKeyAndWindow((str1,str2)->{
               return String.valueOf(Double.parseDouble(str1)+Double.parseDouble(str2));
            },Durations.seconds(60),Durations.seconds(1))
                    .print();


        }

        //updateStateByKey可以实现有状态的流处理，类似的函数还有mapWithState ，如果没有这种机制想要实现类似的效果的话，就要利用Redis或者hdfs之类的第三方储存你，性能会下去很多
    public void testUpdateStateByKey(JavaInputDStream<ConsumerRecord<String, String>> messages){
        JavaPairDStream<String, String> datas = messages.mapToPair(stringStringConsumerRecord -> {
            //     System.out.println(stringStringConsumerRecord.value());
            MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
            //Double.parseDouble(messageDataInfo.getValue())
            return new Tuple2<>(stringStringConsumerRecord.key(),messageDataInfo.getValue() );
        });


        datas.updateStateByKey(
//                (values, state) -> {
//                    Double out = 0.0;
//                    if (state.isPresent()) {
//                       out= out+ (double) state.get();
//                    }
//
//                    for (Double value : values) {
//                        out = out + value;
//                    }
//
//                    //这个Optional的包很容易导错
//                    return Optional.of(out);
//                }

                SparkConsumerService::updateFunc

        ).print();
        //要有这个print或者foreachRDD之类的输出操作，不然最后job不会执行的

    }
    public static Optional<String> updateFunc(List<String> newValue, Optional<String> OldValues){
        double prev = OldValues.isPresent()? Double.parseDouble(OldValues.get()) : 0;
        double v = newValue.stream().map(Double::parseDouble).reduce(0.0,(a,b)->{return a+b;});
        return Optional.of(String.valueOf(prev+v));
    }

    //性能更好，扩展性更高
    public void testMapWithState(JavaInputDStream<ConsumerRecord<String, String>> messages,Broadcast<KafkaProducerClient> kafkaProducer,Broadcast<String> sendTopic){

        JavaPairDStream<String, String> datas = messages.mapToPair(stringStringConsumerRecord -> {
            //     System.out.println(stringStringConsumerRecord.value());
            MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
            //Double.parseDouble(messageDataInfo.getValue())
            return new Tuple2<>(stringStringConsumerRecord.key(),messageDataInfo.getValue() );
        });
        //KeyType, ValueType, StateType, MappedType
        JavaMapWithStateDStream<String, String, String, Tuple2<String, String>> res = datas.mapWithState(StateSpec.function(SparkConsumerService::mapWithStateFunction));
        //stateSnapshots() 这个函数很重要，用来获取最新的快照，不然res会包含一堆以前更新完的数据
        res.stateSnapshots().foreachRDD(
                rdd ->{
                    rdd.sortByKey(true).collect()
                            .forEach(
                                    record->{
                                        System.out.println(record);
                                        KafkaSink.getInstance().send(sendTopic.getValue(),record._1,record._2);
                                      //  kafkaProducer.getValue().send(sendTopic.getValue(),record._1,record._2);
                                      //  System.out.println(myCustomizedConfig.getSendTopic());
                                        //kafkaProducerService.send(myCustomizedConfig.getSendTopic(),record._1,record._2);
                                    }
                            );
                }
        );
    }

    public static Tuple2<String,String> mapWithStateFunction(String keyType, Optional<String> value, State<String> oldValue){
        double sum =0.0;
        if(value.isPresent()){
            sum+= Double.parseDouble(value.get());
        }
        if(oldValue.exists()){

            sum+= Double.parseDouble(oldValue.get());
        }
        oldValue.update(String.valueOf(sum));
        //好处在于可以自己包装模型发送到下游，比如这个tuple，这是updateStateByKey做不到的
        return new Tuple2<String,String>(keyType,String.valueOf(sum));
    }
}
