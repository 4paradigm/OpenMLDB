package com._4paradigm.dataimporter.parseCsv;

import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.dataimporter.verification.CheckParameters;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com.csvreader.CsvReader;
import com._4paradigm.dataimporter.operator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ParseCsvUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseCsvUtil.class);
    private static String csvFilePath = "/Users/innerpeace/Desktop/test.csv";
    private static String tableName = "csvTest";
    private static char separator = ',';//csv文件字符分隔符
    private static String encodingFormat = "UTF-8";//编码格式

    /**
     * @param path  文件的绝对路径
     * @param tableName  表名
     * @param schemaList  保存schema的list
     */
    public static void putCsv(String path,String tableName, List<ColumnDesc> schemaList){
        if (CheckParameters.checkPutParameters(path, tableName, schemaList)) return;
        AtomicLong id = new AtomicLong(1);
        TableSyncClient client ;
        CsvReader reader = null;
        try {
            reader = new CsvReader(csvFilePath, separator, Charset.forName(encodingFormat));
            // 跳过表头 如果需要表头的话，这句可以忽略
            reader.readHeaders();
            // 逐行读入除表头的数据,index决定哪个client执行put操作
            for (int index = 0; reader.readRecord(); index++) {
                logger.info("每行读到的数据为：" + reader.getRawRecord());
                HashMap<String, Object> map = new HashMap<>();
                String columnName;
                for (int j = 0; j < reader.getValues().length; j++) {
                    columnName = schemaList.get(j).getName();
                    switch(columnName){
                        case "int_32":
                            map.put(columnName, Integer.parseInt(reader.getValues()[j]));
                            break;
                        case "int_64":
                            map.put(columnName, Long.parseLong(reader.getValues()[j]));
                            break;
                        case "int_96":
                            map.put(columnName, reader.getValues()[j]);
                            break;
                        case "float_1":
                            map.put(columnName, Float.parseFloat(reader.getValues()[j]));
                            break;
                        case "double_1":
                            map.put(columnName, Double.parseDouble(reader.getValues()[j]));
                            break;
                        case "boolean_1":
                            map.put(columnName, Boolean.parseBoolean(reader.getValues()[j]));
                            break;
                        case "binary_1":
                            map.put(columnName, reader.getValues()[j]);
                            break;
                    }
                }
                if (index == OperateTable.SUM) {
                    index = 0;
                }
                client = OperateTable.getTableSyncClient()[index];
                OperateThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), client, tableName, map));
            }
            OperateThreadPool.getExecutor().shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader!=null){
                reader.close();
            }
        }
    }

    public static void main(String[] args) {
//        CsvExample.writeCSV(csvFilePath);
        OperateTable.initClient();
        OperateThreadPool.initThreadPool();
        OperateTable.dropTable(tableName);
        OperateTable.createSchemaTable(tableName, OperateTable.getRtidbSchema());
        putCsv(csvFilePath,tableName,OperateTable.getRtidbSchema());
    }
}
