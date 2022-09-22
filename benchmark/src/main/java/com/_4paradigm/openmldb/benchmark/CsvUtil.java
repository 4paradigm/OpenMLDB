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
            CsvReader reader = new CsvReader(filePath, ',', Charset.forName("UTF-8"));
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
        lists.forEach(l-> System.out.println(l));
    }
}
