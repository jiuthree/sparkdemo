package com.example.sparkdemo.kafka;

//import com.example.sparkdemo.config.KafkaConsumerConfig;

import com.example.sparkdemo.config.MyCustomizedConfig;
import com.example.sparkdemo.entity.DeviceStatisticsData;
import com.example.sparkdemo.util.MyRedisUtils;
import com.example.sparkdemo.util.TDigestUtils;
import com.example.sparkdemo.vo.GroupInfoMetadata;
import com.example.sparkdemo.vo.MessageDataInfoVo;
import com.google.gson.Gson;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


@Service
public class SparkConsumerService {
    public final static String WindowState = "WindowState";
    public final static String MapState = "MapState";
    public final static String Quantile = "Quantile";
    public final static String AnalysisState = "AnalysisState";
    public static ConcurrentHashMap<String, AVLTreeDigest> AVLTreeDigestMap = new ConcurrentHashMap<>();
    private final Logger log = LoggerFactory.getLogger(SparkConsumerService.class);
    //public static AVLTreeDigest avlTreeDigest = new AVLTreeDigest(100);
    private final SparkConf sparkConf;
    private final MyCustomizedConfig myCustomizedConfig;
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

    public static Optional<String> updateFunc(List<String> newValue, Optional<String> OldValues) {
        double prev = OldValues.isPresent() ? Double.parseDouble(OldValues.get()) : 0;
        double v = newValue.stream().map(Double::parseDouble).reduce(0.0, (a, b) -> {
            return a + b;
        });
        return Optional.of(String.valueOf(prev + v));
    }

    public static Tuple2<String, String> mapWithStateFunction(String keyType, Optional<String> value, State<String> oldValue) {
        double sum = 0.0;
        if (value.isPresent()) {
            sum += Double.parseDouble(value.get());
        }
        if (oldValue.exists()) {

            sum += Double.parseDouble(oldValue.get());
        }
        oldValue.update(String.valueOf(sum));
        //好处在于可以自己包装模型发送到下游，比如这个tuple，这是updateStateByKey做不到的
        return new Tuple2<String, String>(keyType, String.valueOf(sum));
    }

    public static Tuple2<String, Tuple2<String, String>> analysisPowerStateFunction(String keyType, Optional<String> value, State<Tuple2<String, String>> oldValue) {
        String extend = "";
        double power = 0.0;
        double oldPower = 0.0;
        if (value.isPresent()) {
            power = Double.parseDouble(value.get());
        }
        if (oldValue.exists()) {
            extend = oldValue.get()._2();
            oldPower = Double.parseDouble(oldValue.get()._1());
            double gap = Math.abs(oldPower - power);
            //出现了大于40%的波动变化，推测触发了操作
            if (power * 0.4 < gap) {

                if (power - oldPower > 0) {

                    //由0.2倍的gap来推断是否原来是接近0W的
                    if (gap * 0.2 - oldPower > 0) {
                        extend = "设备开启";

                    } else {
                        extend = "设备工作功率升高，可能由非工作状态切换到工作状态";
                    }

                } else {
                    //由0.2倍的gap来推断是否最后是接近0W的
                    if (gap * 0.2 - power > 0) {
                        extend = "设备关闭";

                    } else {
                        extend = "设备工作功率降低，可能由工作状态切换到非工作状态";
                    }


                }
            }
        }
        oldValue.update(new Tuple2<>(String.valueOf(power), extend));
        //好处在于可以自己包装模型发送到下游，比如这个tuple，这是updateStateByKey做不到的
        return new Tuple2<String, Tuple2<String, String>>(keyType, new Tuple2<>(String.valueOf(power), extend));
    }


    public void run() {
        log.debug("Running Spark Consumer Service..");

        // Create context with a 0.2 seconds batch interval
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(1000));
        jssc.checkpoint(myCustomizedConfig.getCheckpointDir());
        jssc.sparkContext().setLogLevel("WARN");
        // jssc.sparkContext().broadcast(kafkaProducerService);
        Broadcast<String> sendTopic = jssc.sparkContext().broadcast(myCustomizedConfig.getSendTopic());
        Broadcast<Double> quantile = jssc.sparkContext().broadcast(Double.parseDouble(myCustomizedConfig.getGetQuantile()));
        KafkaProducerClient kpc = new KafkaProducerClient();

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

        //   testMapWithState(messages, kafkaProducer, sendTopic);

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


        JavaDStream<MessageDataInfoVo> messageDataInfoVoJavaDStream = messages.map(consumerRecord -> {
            MessageDataInfoVo messageDataInfoVo = new Gson().fromJson(consumerRecord.value(), MessageDataInfoVo.class);

            return messageDataInfoVo;
        });

        JavaPairDStream<String, MessageDataInfoVo> messageDataInfoVoJavaPairDStream = messageDataInfoVoJavaDStream.mapToPair(messageDataInfoVo -> {

            return new Tuple2<>(messageDataInfoVo.getKeyInfo().getKey(), messageDataInfoVo);
        });


        //返回的对象是个新的DStream ，老的DStream应该还没变，因为rdd是只读的


        //先filter 看有没有这个WindowState这个操作
        messageDataInfoVoJavaPairDStream.filter(data -> {
                    List<GroupInfoMetadata> metadataList = data._2.getKeyInfo().getGroupInfo().getMetadataList();
                    boolean flag = false;
                    for (GroupInfoMetadata metadata : metadataList) {
                        if (metadata.getValue().equals(WindowState)) {
                            flag = true;
                            break;
                        }

                    }
                    return flag;


                }).mapToPair(data -> {
                            return new Tuple2<>(data._1 + "#" + data._2.getName(), data._2.getValue());
                        }
                )
                .reduceByKeyAndWindow((str1, str2) -> {


                    return String.valueOf(Double.parseDouble(str1) + Double.parseDouble(str2));
                }, Durations.seconds(60), Durations.seconds(1))
                .foreachRDD((rdd, time) -> {
                    rdd.sortByKey(true).collect().forEach(record -> {
                        System.out.println(record);
                        DeviceStatisticsData statisticsData = new DeviceStatisticsData();
                        statisticsData.setValue(record._2);

                        statisticsData.setExtend(String.valueOf(time.milliseconds()));
                        List<String> keyAndName = Arrays.asList(record._1.split("#"));
                        statisticsData.setKeyinfo(keyAndName.get(0));
                        statisticsData.setDescription(WindowState);
                        statisticsData.setName(keyAndName.get(1));
                        String kafkaMes = new Gson().toJson(statisticsData);
                        KafkaSink.getInstance().send(sendTopic.getValue(), WindowState, kafkaMes);

                    });
                    System.out.println(time.milliseconds());
                });

        //这个可以测试那个DStream到底变没变
        //messageDataInfoVoJavaDStream.print();

        messageDataInfoVoJavaPairDStream.filter(data -> {
                    List<GroupInfoMetadata> metadataList = data._2.getKeyInfo().getGroupInfo().getMetadataList();
                    boolean flag = false;
                    for (GroupInfoMetadata metadata : metadataList) {
                        if (metadata.getValue().equals(MapState)) {  //空指针
                            flag = true;
                            break;
                        }
                    }
                    return flag;
                }).mapToPair(data -> {
                    return new Tuple2<>(data._1 + "#" + data._2.getName(), data._2.getValue());
                })
                .mapWithState(StateSpec.function(SparkConsumerService::mapWithStateFunction))
                .stateSnapshots().foreachRDD(
                        rdd -> {
                            rdd.sortByKey(true).collect()
                                    .forEach(
                                            record -> {

                                                System.out.println(record);
                                                DeviceStatisticsData statisticsData = new DeviceStatisticsData();
                                                statisticsData.setValue(record._2);

                                                List<String> keyAndName = Arrays.asList(record._1.split("#"));
                                                statisticsData.setKeyinfo(keyAndName.get(0));
                                                statisticsData.setDescription(MapState);
                                                statisticsData.setName(keyAndName.get(1));
                                                String kafkaMes = new Gson().toJson(statisticsData);
                                                KafkaSink.getInstance().send(sendTopic.getValue(), MapState, kafkaMes);
                                                //   KafkaSink.getInstance().send(sendTopic.getValue(), record._1, record._2);
                                                //  kafkaProducer.getValue().send(sendTopic.getValue(),record._1,record._2);
                                                //  System.out.println(myCustomizedConfig.getSendTopic());
                                                //kafkaProducerService.send(myCustomizedConfig.getSendTopic(),record._1,record._2);
                                            }
                                    );
                        }
                );


        //先filter 看有没有这个WindowState这个操作
        messageDataInfoVoJavaPairDStream.filter(data -> {
                    List<GroupInfoMetadata> metadataList = data._2.getKeyInfo().getGroupInfo().getMetadataList();
                    boolean flag = false;
                    for (GroupInfoMetadata metadata : metadataList) {
                        if (metadata.getValue().equals(AnalysisState)) {
                            flag = true;
                            break;
                        }

                    }
                    return flag;


                }).mapToPair(data -> {
                            return new Tuple2<>(data._1 + "#" + data._2.getName(), data._2.getValue());
                        }
                )
                .mapWithState(StateSpec.function(SparkConsumerService::analysisPowerStateFunction))
                .stateSnapshots().foreachRDD(
                        rdd -> {
                            rdd.sortByKey(true).collect()
                                    .forEach(
                                            record -> {

                                                System.out.println(record);
                                                DeviceStatisticsData statisticsData = new DeviceStatisticsData();
                                                statisticsData.setValue(record._2._1());

                                                List<String> keyAndName = Arrays.asList(record._1.split("#"));
                                                statisticsData.setKeyinfo(keyAndName.get(0));
                                                statisticsData.setDescription(AnalysisState);
                                                statisticsData.setExtend(record._2._2());
                                                statisticsData.setName(keyAndName.get(1));
                                                String kafkaMes = new Gson().toJson(statisticsData);
                                                KafkaSink.getInstance().send(sendTopic.getValue(), AnalysisState, kafkaMes);
                                                //   KafkaSink.getInstance().send(sendTopic.getValue(), record._1, record._2);
                                                //  kafkaProducer.getValue().send(sendTopic.getValue(),record._1,record._2);
                                                //  System.out.println(myCustomizedConfig.getSendTopic());
                                                //kafkaProducerService.send(myCustomizedConfig.getSendTopic(),record._1,record._2);
                                            }
                                    );
                        }
                );


        messageDataInfoVoJavaPairDStream.filter(data -> {
                    List<GroupInfoMetadata> metadataList = data._2.getKeyInfo().getGroupInfo().getMetadataList();
                    boolean flag = false;
                    for (GroupInfoMetadata metadata : metadataList) {
                        if (metadata.getValue().equals(Quantile)) {  //空指针
                            flag = true;
                            break;
                        }
                    }
                    return flag;
                }).mapToPair(data -> {
                    return new Tuple2<>(data._1 + "#" + data._2.getName(), new Tuple2<>(data._1 + "#" + data._2.getName(), data._2.getValue()));
                    //   return new Tuple2<>(data._1, new Tuple2<>(data._1, data._2.getValue()));
                }).reduceByKey(
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
                )
                .map((tuple2) -> {
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
                    //    System.out.println((("update!!!")));
                    //    System.out.println(("size:" + redisDigest.size()));
                    //System.out.println(TDigestUtils.getQuantile(redisDigest, 0.9));


                    DeviceStatisticsData statisticsData = new DeviceStatisticsData();
                    statisticsData.setExtend("size:" + redisDigest.size());
                    statisticsData.setValue(String.valueOf(TDigestUtils.getQuantile(redisDigest, quantile.getValue())));

                    List<String> keyAndName = Arrays.asList(tuple2._1.split("#"));
                    statisticsData.setKeyinfo(keyAndName.get(0));
                    statisticsData.setDescription(Quantile);
                    statisticsData.setName(keyAndName.get(1));
                    String kafkaMes = new Gson().toJson(statisticsData);
                    KafkaSink.getInstance().send(sendTopic.getValue(), Quantile, kafkaMes);


                    return Quantile;
                }).print();
        ;


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
        datas.reduceByKeyAndWindow((str1, str2) -> {
                    return String.valueOf(Double.parseDouble(str1) + Double.parseDouble(str2));
                }, Durations.seconds(60), Durations.seconds(1))
                .print();


    }

    //updateStateByKey可以实现有状态的流处理，类似的函数还有mapWithState ，如果没有这种机制想要实现类似的效果的话，就要利用Redis或者hdfs之类的第三方储存你，性能会下去很多
    public void testUpdateStateByKey(JavaInputDStream<ConsumerRecord<String, String>> messages) {
        JavaPairDStream<String, String> datas = messages.mapToPair(stringStringConsumerRecord -> {
            //     System.out.println(stringStringConsumerRecord.value());
            MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
            //Double.parseDouble(messageDataInfo.getValue())
            return new Tuple2<>(stringStringConsumerRecord.key(), messageDataInfo.getValue());
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

    //性能更好，扩展性更高
    public void testMapWithState(JavaInputDStream<ConsumerRecord<String, String>> messages, Broadcast<KafkaProducerClient> kafkaProducer, Broadcast<String> sendTopic) {

        JavaPairDStream<String, String> datas = messages.mapToPair(stringStringConsumerRecord -> {
            //     System.out.println(stringStringConsumerRecord.value());
            MessageDataInfoVo messageDataInfo = new Gson().fromJson(stringStringConsumerRecord.value(), MessageDataInfoVo.class);
            //Double.parseDouble(messageDataInfo.getValue())
            return new Tuple2<>(stringStringConsumerRecord.key(), messageDataInfo.getValue());
        });
        //KeyType, ValueType, StateType, MappedType
        JavaMapWithStateDStream<String, String, String, Tuple2<String, String>> res = datas.mapWithState(StateSpec.function(SparkConsumerService::mapWithStateFunction));
        //stateSnapshots() 这个函数很重要，用来获取最新的快照，不然res会包含一堆以前更新完的数据
        res.stateSnapshots().foreachRDD(
                rdd -> {
                    rdd.sortByKey(true).collect()
                            .forEach(
                                    record -> {
                                        System.out.println(record);
                                        KafkaSink.getInstance().send(sendTopic.getValue(), record._1, record._2);
                                        //  kafkaProducer.getValue().send(sendTopic.getValue(),record._1,record._2);
                                        //  System.out.println(myCustomizedConfig.getSendTopic());
                                        //kafkaProducerService.send(myCustomizedConfig.getSendTopic(),record._1,record._2);
                                    }
                            );
                }
        );
    }
}
