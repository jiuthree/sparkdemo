package com.example.sparkdemo;

import com.example.sparkdemo.dao.ElectricityPB2EDao;
import com.example.sparkdemo.dao.ElectricityPDao;
import com.example.sparkdemo.dao.ElectricityPHPEDao;
import com.example.sparkdemo.entity.ElectricityP;
import com.example.sparkdemo.entity.ElectricityPB2E;
import com.example.sparkdemo.entity.ElectricityPHPE;
import com.example.sparkdemo.util.CsvUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;



@RunWith(SpringRunner.class)
@SpringBootTest(classes = SparkdemoApplication.class)
public class AddDataTest {
    @Resource
    ElectricityPB2EDao electricityPB2EDao;

    @Resource
    ElectricityPHPEDao electricityPHPEDao;

    @Resource
    ElectricityPDao electricityPDao;

    @Test
    public void contextLoads() throws Exception {
        String csvPath = "E:\\GraduationPro\\PythonPro\\pro1\\AnomalyDetection-master\\resources\\dataset\\Electricity_PHPE.csv";
        List<ElectricityPHPE> datas = CsvUtil.convertCsvFileToObjects(ElectricityPHPE.class, csvPath);
        System.out.println(datas.size());
        electricityPHPEDao.batchCreate(datas);
    }

    @Test
    public void addDataToClickHouse() throws Exception {
        String csvPath = "E:\\GraduationPro\\PythonPro\\pro1\\AnomalyDetection-master\\resources\\dataset\\Electricity_PB2E.csv";
        List<ElectricityPB2E> datas = CsvUtil.convertCsvFileToObjects(ElectricityPB2E.class, csvPath);
        System.out.println(datas.size());
        electricityPB2EDao.batchCreate(datas);
    }
    @Test
    public void addDataToClickHouse2() throws Exception {
        String csvPath = "E:\\GraduationPro\\PythonPro\\pro1\\AnomalyDetection-master\\resources\\dataset\\Electricity_PHPE.csv";
        List<ElectricityPHPE> datas = CsvUtil.convertCsvFileToObjects(ElectricityPHPE.class, csvPath);
        System.out.println(datas.size());
        electricityPHPEDao.batchCreate(datas);
    }

    @Test
    public void addDataToClickHouse3() throws Exception {
        String csvPath = "E:\\GraduationPro\\PythonPro\\pro1\\AnomalyDetection-master\\resources\\dataset\\Electricity_P.csv";
        List<ElectricityP> datas = CsvUtil.convertCsvFileToObjects(ElectricityP.class, csvPath);
        System.out.println(datas.size());
        electricityPDao.batchCreate(datas);
    }
}
