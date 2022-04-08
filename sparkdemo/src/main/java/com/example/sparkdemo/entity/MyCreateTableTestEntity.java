package com.example.sparkdemo.entity;

import asalty.fish.clickhousejpa.annotation.ClickHouseColumn;
import asalty.fish.clickhousejpa.annotation.ClickHouseEngine;
import asalty.fish.clickhousejpa.annotation.ClickHouseEntity;
import asalty.fish.clickhousejpa.annotation.ClickHouseTable;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@ClickHouseEntity
@ClickHouseTable(name = "create_table_test_entity", engine = ClickHouseEngine.MergeTree)
public class MyCreateTableTestEntity {

    @ClickHouseColumn(isPrimaryKey = true)
    public Long id;

    @ClickHouseColumn(comment = "观看id")
    public Long WatchID;

    public Boolean JavaEnable;

    @ClickHouseColumn(comment = "标题")
    public String Title;

    public String GoodEvent;

    public Integer UserAgentMajor;

    @ClickHouseColumn(name = "URLDomain")
    public String testUserDefinedColumn;

    public LocalDateTime CreateTime;

    public LocalDate CreateDay;

    public MyCreateTableTestEntity() {

    }
}
