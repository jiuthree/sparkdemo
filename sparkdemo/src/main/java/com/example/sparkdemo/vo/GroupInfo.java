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

    // 一种设计是避开所谓的套娃结构，就用数组结构 这个groupinfo数组中的每一个info，代表了一个处理过程，然后这些处理流程是无所谓先后的，是并发的
    //因此会损失一定的计算效率，有重复计算的部分，但是代码层面就简洁了很多，对于每个info信息，直接设计一个比较长的key，处理的时候，一个for循环， 一步到位
    //key作为分组的信息，value作为要调用的函数名，而数据本身还有一个value，就是要统计的量
    private final List<GroupInfoMetadata> metadataList = new ArrayList<>();

    public GroupInfo addMetaData(GroupInfoMetadata metadata){
        if (metadata == null) {
            throw new IllegalArgumentException("GroupInfoMetadata cannot be null");
        }
        this.metadataList.add(metadata);
        return this;
    }

    public GroupInfoVo toGroupInfoVo(){
        GroupInfoVo groupInfoVo = new GroupInfoVo();
        groupInfoVo.setInfoId(this.infoId);
        groupInfoVo.setMetadataList(this.metadataList);
        return groupInfoVo;
    }

    //todo 考虑生成一个唯一的key信息来辅助后续reduce，或者分层级reduce
}
