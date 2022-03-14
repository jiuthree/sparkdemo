package com.example.sparkdemo.util;


import com.example.sparkdemo.SparkdemoApplication;
import com.example.sparkdemo.entity.TDigestDataStruct;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

public class MyRedisUtils {

    public static synchronized TDigest aggregateAndSaveToRedis(TDigest tDigest,String key){
        TDigestDataStruct tDigestDataStruct = SparkdemoApplication.TDigestDataMap.get(key);
        if(tDigestDataStruct==null){
            //原本没有，不用merge了，直接save到redis然后返回
            TDigestDataStruct tDigestDS = TDigestUtils.transferToAVLTDigestDS(tDigest, key);
            SparkdemoApplication.TDigestDataMap.put(key,tDigestDS);
            return tDigest;
        }else {
            TDigest tDigestOfRedis = TDigestUtils.transferToAVLTDigest(tDigestDataStruct, key);
            tDigest.add(tDigestOfRedis);
            TDigestDataStruct tDigestDS = TDigestUtils.transferToAVLTDigestDS(tDigest, key);
            SparkdemoApplication.TDigestDataMap.put(key,tDigestDS);
            return tDigest;
        }
    }
}
