package com._4paradigm.dataimporter.parseUtil;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private String filePath;
    private String tableName;
    private List<String[]> scheamInfo;
    private static final String INDEX = Constant.INDEX;
    private final String TIMESTAMP = Constant.TIMESTAMP;
    private Long timestamp = null;

    public ParseCsvUtil(String filePath, String tableName, List<String[]> scheamInfo) {
        this.filePath = filePath;
        this.tableName = tableName;
        this.scheamInfo = scheamInfo;
    }

    private HashMap<String, Object> read(CsvReader reader) {
        HashMap<String, Object> map = new HashMap<>();
        String columnName;
        String columnType;
        int columnIndex = 0;
        for (String[] string : scheamInfo) {
            columnName = string[0].split("=")[1].trim();
            columnType = string[1].split("=")[1].trim();
            if (string.length == 3) {
                columnIndex = Integer.valueOf(string[2].split("=")[1].trim());
            }
            try {
                switch (columnType) {
                    case "int32":
                        map.put(columnName, Integer.parseInt(reader.getValues()[columnIndex]));
                        break;
                    case "int64":
                        map.put(columnName, Long.parseLong(reader.getValues()[columnIndex]));
                        break;
                    case "string":
                        map.put(columnName, reader.getValues()[columnIndex]);
                        break;
                    case "float":
                        map.put(columnName, Float.parseFloat(reader.getValues()[columnIndex]));
                        break;
                    case "double":
                        map.put(columnName, Double.parseDouble(reader.getValues()[columnIndex]));
                        break;
                    case "boolean":
                        map.put(columnName, Boolean.parseBoolean(reader.getValues()[columnIndex]));
                        break;
                    default:
                }
                if (columnName.equals(TIMESTAMP)) {
                    timestamp = Long.parseLong(reader.getValues()[columnIndex]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (string.length != 3) {
                columnIndex++;
            }
        }
        return map;
    }

    public void put() {
        AtomicLong id = new AtomicLong(1);
        CsvReader reader = null;
        try {
            reader = new CsvReader(filePath, Constant.CSV_SEPARATOR.toCharArray()[0], Charset.forName(Constant.CSV_ENCODINGFORMAT));
            // 跳过表头 如果需要表头的话，这句可以忽略
            reader.readHeaders();
            for (int clientIndex = 0; reader.readRecord(); clientIndex++) {
                logger.debug("read data：{}", reader.getRawRecord());
                HashMap<String, Object> map = read(reader);
                if (clientIndex == InitClient.MAX_THREAD_NUM) {
                    clientIndex = 0;
                }
                TableSyncClient client = InitClient.getTableSyncClient()[clientIndex];
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), timestamp, client, tableName, map));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public static List<String[]> getSchema(String schemaPath) {
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

    public static List<ColumnDesc> getSchemaOfRtidb(List<String[]> schemaList) {
        List<ColumnDesc> list = new ArrayList<>();
        String columnName;
        String type;
        for (String[] string : schemaList) {
            ColumnDesc columnDesc = new ColumnDesc();
            if (INDEX.contains(string[0].split("=")[1])) {
                columnDesc.setAddTsIndex(true);
            }
            columnName = string[0].split("=")[1];
            type = string[1].split("=")[1];
            switch (type) {
                case "int32":
                    columnDesc.setType(ColumnType.kInt32);
                    break;
                case "int64":
                    columnDesc.setType(ColumnType.kInt64);
                    break;
                case "string":
                    columnDesc.setType(ColumnType.kString);
                    break;
                case "boolean":
                    columnDesc.setType(ColumnType.kBool);
                    break;
                case "float":
                    columnDesc.setType(ColumnType.kFloat);
                    break;
                case "double":
                    columnDesc.setType(ColumnType.kDouble);
                    break;
                default:

            }
            columnDesc.setName(columnName);
            list.add(columnDesc);
        }
        return list;
    }
}
