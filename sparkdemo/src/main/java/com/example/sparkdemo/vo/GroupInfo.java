package com.example.sparkdemo.vo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupInfo {
    String infoId;
    private final List<GroupInfoMetadata> metadataList = new ArrayList<>();

    public GroupInfo addMetaData(GroupInfoMetadata metadata){
        if (metadata == null) {
            throw new IllegalArgumentException("GroupInfoMetadata cannot be null");
        }
        this.metadataList.add(metadata);
        return this;
    }

    //todo 考虑生成一个唯一的key信息来辅助后续reduce，或者分层级reduce
}
