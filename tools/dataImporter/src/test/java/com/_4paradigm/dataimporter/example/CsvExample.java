package com._4paradigm.dataimporter.example;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class CsvExample {
    public static void writeCSV(String csvFilePath) {
        try {
            // 创建CSV写对象 例如:CsvWriter(文件路径，分隔符，编码格式);
            CsvWriter csvWriter = new CsvWriter(csvFilePath, ',', Charset.forName("UTF-8"));
            // 写表头
            String[] csvHeaders = {"int_16", "int_32", "int_64", "string", "float_1", "double_1", "boolean_1", "date", "timestamp"};
            csvWriter.writeRecord(csvHeaders);
            // 写内容
            String[] arr = {"1", "22", "33", "ss", "5", "6", "true", "1994-10-08", "1994-10-08 00:00:00"};
            for (int i = 0; i < 2; i++) {
                String[] csvContent = {arr[0], arr[1], arr[2], arr[3], arr[4], arr[5], arr[6], arr[7], arr[8]};
                csvWriter.writeRecord(csvContent);
            }
            csvWriter.close();
            System.out.println("--------CSV文件已经写入--------");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readCSV(String csvFilePath) {
        try {
            // 用来保存数据
            ArrayList<String[]> csvFileList = new ArrayList<>();
            // 创建CSV读对象 例如:CsvReader(文件路径，分隔符，编码格式);
            CsvReader reader = new CsvReader(csvFilePath, ',', Charset.forName("UTF-8"));
            // 跳过表头 如果需要表头的话，这句可以忽略
            reader.readHeaders();
            // 逐行读入除表头的数据
            while (reader.readRecord()) {
                System.out.println(reader.getRawRecord());
                System.out.println(reader.getValues()[0]);
                csvFileList.add(reader.getValues());
            }
            reader.close();

            // 遍历读取的CSV文件
            for (int row = 0; row < csvFileList.size(); row++) {
                // 取得第row行第0列的数据
                String cell = csvFileList.get(row)[0];
                System.out.println("------------>" + cell);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        writeCSV("/Users/innerpeace/Desktop/test.csv");
    }
}
