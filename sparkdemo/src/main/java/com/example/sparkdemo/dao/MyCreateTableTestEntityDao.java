package com.example.sparkdemo.dao;

import asalty.fish.clickhousejpa.annotation.ClickHouseNativeQuery;
import asalty.fish.clickhousejpa.annotation.ClickHouseRepository;

import com.example.sparkdemo.entity.MyCreateTableTestEntity;
import org.springframework.stereotype.Component;

import java.util.List;


@ClickHouseRepository(entity = MyCreateTableTestEntity.class)
public class MyCreateTableTestEntityDao {

    public List<MyCreateTableTestEntity> findAllByWatchID(Long watchID) {
        return null;
    }

    public Boolean create(MyCreateTableTestEntity entity) {
        return null;
    }

    @ClickHouseNativeQuery("select count(*) from create_table_test_entity")
    public Long countAll() {
        return null;
    }

    public Long countWatchIDByWatchID(Long watchID) {
        return null;
    }

    public Long countWatchIDByWatchIDAndTitle(Long watchID, String title) {
        return null;
    }
}
