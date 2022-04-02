package com.example.sparkdemo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GroupInfoMetadata implements Serializable {
    String key; //分组信息
    String value;  //方法名
}
