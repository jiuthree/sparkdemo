package com.example.sparkdemo.util;

import com.example.sparkdemo.entity.TDigestDataStruct;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

import java.nio.ByteBuffer;

public class TDigestUtils {
    public static TDigestDataStruct transferToAVLTDigestDS(TDigest tDigest,String key){
        TDigestDataStruct tDigestDataStruct = new TDigestDataStruct();
        tDigestDataStruct.setKey(key);
        ByteBuffer buffer = ByteBuffer.allocate(tDigest.byteSize());
        tDigest.asBytes(buffer);
        tDigestDataStruct.setSerializedTDigest(buffer.array());
        return tDigestDataStruct;
    }

    public static TDigest transferToAVLTDigest(TDigestDataStruct tDigestDataStruct, String key){
        return AVLTreeDigest.fromBytes(ByteBuffer.wrap(tDigestDataStruct.getSerializedTDigest()));
    }

    public static double getQuantile(TDigest tDigest, double v){
        return tDigest.quantile(v);
    }

    public static double getCdf(TDigest tDigest,double v){
        return tDigest.cdf(v);
    }


}
