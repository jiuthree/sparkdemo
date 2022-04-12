package com.example.sparkdemo.dao;

import asalty.fish.clickhousejpa.annotation.ClickHouseNativeQuery;
import asalty.fish.clickhousejpa.annotation.ClickHouseRepository;
import com.example.sparkdemo.entity.ElectricityP;
import com.example.sparkdemo.entity.ElectricityPB2E;

import java.util.List;

@ClickHouseRepository(entity = ElectricityP.class)
public class ElectricityPDao {
    public void batchCreate(List<ElectricityP> list){return ;}
    @ClickHouseNativeQuery("select count(*) from electricty_p_whole")
    public Long countAll() {
        return null;
    }
}
