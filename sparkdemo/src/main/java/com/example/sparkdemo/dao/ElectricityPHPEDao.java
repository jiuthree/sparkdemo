package com.example.sparkdemo.dao;

import asalty.fish.clickhousejpa.annotation.ClickHouseNativeQuery;
import asalty.fish.clickhousejpa.annotation.ClickHouseRepository;
import com.example.sparkdemo.entity.ElectricityPB2E;
import com.example.sparkdemo.entity.ElectricityPHPE;

import java.util.List;

@ClickHouseRepository(entity = ElectricityPHPE.class)
public class ElectricityPHPEDao {
    public void batchCreate(List<ElectricityPHPE> list){return ;}
    @ClickHouseNativeQuery("select count(*) from electricty_p_hpe")
    public Long countAll() {
        return null;
    }
}
