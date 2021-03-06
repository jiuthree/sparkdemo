package com.example.sparkdemo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyInfoVo implements Serializable {
    GroupInfoVo groupInfo;
    String key;
    String extend;
}
