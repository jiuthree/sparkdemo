package com.example.sparkdemo.entity;


import asalty.fish.clickhousejpa.annotation.ClickHouseColumn;
import asalty.fish.clickhousejpa.annotation.ClickHouseEngine;
import asalty.fish.clickhousejpa.annotation.ClickHouseEntity;
import asalty.fish.clickhousejpa.annotation.ClickHouseTable;
import lombok.Data;

@Data
@ClickHouseEntity
@ClickHouseTable(name = "electricty_p_whole", engine = ClickHouseEngine.MergeTree)
public class ElectricityP {
    @ClickHouseColumn(isPrimaryKey = true)
    public Long UNIX_TS;
    public String WHE;
    public String UP;
    public String DOWN;
    public String BEFORE;
    public String AFTER;

    public ElectricityP() {

    }
}