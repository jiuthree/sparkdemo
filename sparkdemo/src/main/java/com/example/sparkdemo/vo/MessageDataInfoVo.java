package com.example.sparkdemo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MessageDataInfoVo implements Serializable {
    String name; //数据的名字，比如电量
    KeyInfoVo keyInfo;
    String value; //其实是double类型，代表了值
}
