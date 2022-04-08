package com.example.sparkdemo.util;

import com.opencsv.CSVReader;

import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 13090
 * @version 1.0
 * @description: TODO
 * @date 2022/3/26 19:45
 */

public class CsvUtil {
    static int row = 0;
    public static <T> T convertCsvLineToObject(Class<T> clazz, String[] csvLine) throws Exception {
        row = 0;
        T object = clazz.newInstance();
        Field[] fields = clazz.getDeclaredFields();
        if (csvLine.length != fields.length) {
            return null;
        }
        for (int i = 0; i < csvLine.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);
            Class<?> fieldType = field.getType();
            String value = csvLine[i];
            if (fieldType == String.class) {
                field.set(object, value);
            } else if (fieldType == int.class || fieldType == Integer.class) {
                field.set(object, Integer.parseInt(value));
            } else if (fieldType == long.class || fieldType == Long.class) {
                field.set(object, Long.parseLong(value));
            } else if (fieldType == float.class || fieldType == Float.class) {

            } else if (fieldType == double.class || fieldType == Double.class) {
                field.set(object, Double.parseDouble(value));
            } else {
                throw new Exception("unsupported type");
            }
            row++;
        }
        return object;
    }

    public static <T> List<T> convertCsvFileToObjects(Class<T> clazz, String csvFilePath) throws Exception {
        List<T> objects = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));
             CSVReader csvReader = new CSVReader(reader)) {
            String[] csvLine;
            // jump header
            csvReader.readNext();
            while ((csvLine = csvReader.readNext()) != null) {
                objects.add(convertCsvLineToObject(clazz, csvLine));
            }
        }
        return objects;
    }
}
