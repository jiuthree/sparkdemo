package com.example.sparkdemo.service;

import com.example.sparkdemo.dao.DeviceStatisticsDataDao;
import com.example.sparkdemo.entity.DeviceStatisticsData;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

@Service
public class DeviceStatisticsDataService {


    @Resource
    DeviceStatisticsDataDao deviceStatisticsDataDao;

    @Transactional
    public DeviceStatisticsData save(DeviceStatisticsData statisticsData){
        if(statisticsData == null)return null;

        DeviceStatisticsData deviceStatisticsDataOfDB = deviceStatisticsDataDao.findDeviceStatisticsDataByKeyinfoAndNameAndDescription(statisticsData.getKeyinfo(), statisticsData.getName(),statisticsData.getDescription());
        if(deviceStatisticsDataOfDB == null){
        return deviceStatisticsDataDao.save(statisticsData);}
        else {
            deviceStatisticsDataOfDB.setValue(statisticsData.getValue());
            deviceStatisticsDataOfDB.setExtend(statisticsData.getExtend());
            deviceStatisticsDataOfDB.setDescription(statisticsData.getDescription());
            return deviceStatisticsDataDao.save(deviceStatisticsDataOfDB);
        }
    }

}
