package com.example.sparkdemo.dao;

import asalty.fish.clickhousejpa.annotation.ClickHouseNativeQuery;
import asalty.fish.clickhousejpa.annotation.ClickHouseRepository;
import com.example.sparkdemo.entity.ElectricityPB2E;
import com.example.sparkdemo.entity.MyCreateTableTestEntity;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;


@ClickHouseRepository(entity = ElectricityPB2E.class)
public class ElectricityPB2EDao {
    public void batchCreate(List<ElectricityPB2E> list){return ;}
    @ClickHouseNativeQuery("select count(*) from electricty_p_bedroom")
    public Long countAll() {
        return null;
    }
}
