package com.example.sparkdemo.dao;

import com.example.sparkdemo.entity.DeviceStatisticsData;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


public interface DeviceStatisticsDataDao extends JpaRepository<DeviceStatisticsData,Long> {

    public DeviceStatisticsData findDeviceStatisticsDataByKeyinfoAndNameAndDescription(String keyinfo,String name,String description);
}
