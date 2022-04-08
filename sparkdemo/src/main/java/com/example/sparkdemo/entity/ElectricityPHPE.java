package com.example.sparkdemo.entity;

import asalty.fish.clickhousejpa.annotation.ClickHouseColumn;
import asalty.fish.clickhousejpa.annotation.ClickHouseEngine;
import asalty.fish.clickhousejpa.annotation.ClickHouseEntity;
import asalty.fish.clickhousejpa.annotation.ClickHouseTable;
import lombok.Data;

@Data
@ClickHouseEntity
@ClickHouseTable(name = "electricty_p_hpe", engine = ClickHouseEngine.MergeTree)
public class ElectricityPHPE {
    @ClickHouseColumn(isPrimaryKey = true)
    public Long UNIX_TS;
    public String HPE;
    public String UP;
    public String DOWN;
    public String BEFORE;
    public String AFTER;

    public ElectricityPHPE() {

    }
}
