package com._4paradigm.openmldb.benchmark;

import com.csvreader.CsvReader;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CsvUtil {
    public static List<List<String>> readCsvByCsvReader(String filePath) {
        List<List<String>> list = new ArrayList<>();
        try {
            CsvReader reader;
            if (filePath.startsWith("/")) {
                reader = new CsvReader(filePath, ',', Charset.forName("UTF-8"));
            }else{
                reader = new CsvReader(CsvUtil.class.getClassLoader().getResourceAsStream(filePath), ',', Charset.forName("UTF-8"));
            }
            reader.readHeaders();
            while (reader.readRecord()) {
                String[] values = reader.getValues();
                List<String> dataList = Arrays.asList(values);
                list.add(dataList);  // 按行读取，并把每一行的数据添加到list集合
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    public static void main(String[] args) {
        List<List<String>> lists = readCsvByCsvReader("/Users/zhaowei/code/4paradigm/OpenMLDB/benchmark/src/main/resources/data/bank_flattenRequest.csv");
        System.out.println("lists.size() = " + lists.size());
        lists.stream().limit(10).forEach(l-> System.out.println(l));
    }
}
