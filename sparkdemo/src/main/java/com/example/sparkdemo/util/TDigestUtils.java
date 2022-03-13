package com.example.sparkdemo.util;

import com.example.sparkdemo.entity.TDigestDataStruct;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;

import java.nio.ByteBuffer;

public class TDigestUtils {
    public TDigestDataStruct transferToAVLTDigestDS(TDigest tDigest,String key){
        TDigestDataStruct tDigestDataStruct = new TDigestDataStruct();
        tDigestDataStruct.setKey(key);
        ByteBuffer buffer = ByteBuffer.allocate(tDigest.byteSize());
        tDigest.asBytes(buffer);
        tDigestDataStruct.setSerializedTDigest(buffer.array());
        return tDigestDataStruct;
    }

    public TDigest transferToAVLTDigest(TDigestDataStruct tDigestDataStruct){
        return AVLTreeDigest.fromBytes(ByteBuffer.wrap(tDigestDataStruct.getSerializedTDigest()));
    }


}
