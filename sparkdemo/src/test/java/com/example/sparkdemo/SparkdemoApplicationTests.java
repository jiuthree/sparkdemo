package com.example.sparkdemo;


import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.ByteBuffer;

@SpringBootTest
public class SparkdemoApplicationTests {

    @Test
    public void contextLoads() {
        //当 compression 参数设置越大时，聚类得到的质心越多，则差分法求取的分位数精确度越高；
        //数据量越大，compression参数应该越大
        //100 是正常使用的常用值。1000 非常大。保留的质心数量将是该数量的一小部分（通常小于 10）

        TDigest tDigest = new AVLTreeDigest(100);
        for(int i=0;i<10;i++){
            tDigest.add(i);
        }
        System.out.println(tDigest.compression());
    }

    @Test
    public void mytest(){
        TDigest tDigest = new AVLTreeDigest(0.99);

        for(int i=0;i<10000;i++){
            tDigest.add(i);
        }

        //传入百分数，返回浮点数
        System.out.println(tDigest.quantile(0.9));
        //传入浮点数，返回百分数
        System.out.println(tDigest.cdf(9000));
        ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());

        //序列化到这个bytebuffer中
        tDigest.asBytes(byteBuffer);
        byte[] array = byteBuffer.array();


        //中间要转一次数组，不然会解析错误  后续这个数组会存在Redis里面

        ByteBuffer buffer = ByteBuffer.wrap(array);
        //从bytebuffer中还原回来
        AVLTreeDigest digest = AVLTreeDigest.fromBytes(buffer);

        System.out.println(digest.quantile(0.9));
        System.out.println(digest.cdf(9000));
//        for(byte b: byteBuffer.array()){
//            System.out.println(b);
//        }

        // System.out.println(tDigest.centroidCount());
       // System.out.println(tDigest.recordAllData());
    }

}
