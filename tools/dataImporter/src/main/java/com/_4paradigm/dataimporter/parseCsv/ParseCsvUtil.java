package com._4paradigm.dataimporter.parseCsv;

import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.dataimporter.verification.CheckParameters;
import com._4paradigm.rtidb.client.TableSyncClient;
import com.csvreader.CsvReader;
import com._4paradigm.dataimporter.initialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ParseCsvUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseCsvUtil.class);

    /**
     * @param path
     * @param tableName
     * @fuction put data into rtidb
     */
    public static void putCsv(String path, String tableName, List<String[]> scheamInfo) {
        if (CheckParameters.checkPutParameters(path, tableName, scheamInfo)) return;
        AtomicLong id = new AtomicLong(1);
        TableSyncClient client;
        CsvReader reader = null;
        try {
            reader = new CsvReader(path, Constant.CSV_SEPARATOR.toCharArray()[0], Charset.forName(Constant.CSV_ENCODINGFORMAT));
            // 跳过表头 如果需要表头的话，这句可以忽略
            reader.readHeaders();
            // 逐行读入除表头的数据,index决定哪个client执行put操作
            for (int count = 0; reader.readRecord(); count++) {
                logger.debug("read data：" + reader.getRawRecord());
                HashMap<String, Object> map = new HashMap<>();
                String columnName;
                String type;
                int index;
                for (String[] string : scheamInfo) {
                    columnName = string[0].split("=")[1];
                    type = string[1].split("=")[1];
                    index = Integer.valueOf(string[2].split("=")[1]);
                    switch (type) {
                        case "int32":
                            map.put(columnName, Integer.parseInt(reader.getValues()[index]));
                            break;
                        case "int64":
                            map.put(columnName, Long.parseLong(reader.getValues()[index]));
                            break;
                        case "string":
                            map.put(columnName, reader.getValues()[index]);
                            break;
                        case "float":
                            map.put(columnName, Float.parseFloat(reader.getValues()[index]));
                            break;
                        case "double":
                            map.put(columnName, Double.parseDouble(reader.getValues()[index]));
                            break;
                        case "boolean":
                            map.put(columnName, Boolean.parseBoolean(reader.getValues()[index]));
                            break;
                    }
                }
                if (count == InitClient.CLIENTCOUNT) {
                    count = 0;
                }
                client = InitClient.getTableSyncClient()[count];
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), client, tableName, map));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    private static List<String[]> readSchemaFile(String schemaPath) {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get(schemaPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (lines == null) {
            return null;
        }
        List<String[]> arr = new ArrayList<>();
        for (String string : lines) {
            String[] strs = string.split(";");
            arr.add(strs);
        }
        return arr;
    }

    public static void main(String[] args) {
        InitAll.init();
        List<String[]> scheamInfo = readSchemaFile(InitProperties.getProperties().getProperty("csv.schemaPath"));
        if (scheamInfo == null) {
            logger.info("the schemaInfo is null");
            return;
        }
        InitClient.dropTable(Constant.CSV_TABLENAME);
        InitClient.createSchemaTable(Constant.CSV_TABLENAME, InitClient.getSchemaOfRtidb(scheamInfo));

        File rootFile = new File(Constant.CSV_FILEPATH);
        if (rootFile.isDirectory()) {
            List<java.nio.file.Path> filePaths = new ArrayList<>();
            File[] files = rootFile.listFiles();
            if (files == null) {
                logger.info("there is no file in the directory " + rootFile);
                return;
            }
            for (File file : files) {
                filePaths.add(file.toPath());
                logger.info("file path is : " + file.toPath().toString());
            }
            for (java.nio.file.Path filePath : filePaths) {
                putCsv(filePath.toString(), Constant.CSV_TABLENAME, scheamInfo);
            }
        } else if (rootFile.isFile()) {
            putCsv(Constant.CSV_FILEPATH, Constant.CSV_TABLENAME, scheamInfo);
        }
        InitThreadPool.getExecutor().shutdown();
        while (!InitThreadPool.getExecutor().isTerminated()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("The number of successfully inserted is : " + PutTask.successfulCount);
        logger.info("The number of unsuccessfully inserted is : " + PutTask.unSuccessfulCount);
    }
}
