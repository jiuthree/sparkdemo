package com.example.sparkdemo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GroupInfoVo implements Serializable {
    String infoId;
    List<GroupInfoMetadata> metadataList;
}
