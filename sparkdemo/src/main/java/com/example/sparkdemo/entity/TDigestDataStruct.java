package com.example.sparkdemo.entity;

import com.tdunning.math.stats.TDigest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
//默认走AVL的Digest实现类去序列化
public class TDigestDataStruct {
    String key;

    byte[] serializedTDigest;
    String extend;

}
