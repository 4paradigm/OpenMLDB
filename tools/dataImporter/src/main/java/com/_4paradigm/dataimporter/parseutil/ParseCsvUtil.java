package com._4paradigm.dataimporter.parseutil;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ParseCsvUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseCsvUtil.class);
    private String filePath;
    private String tableName;
    private List<String[]> scheamInfo;
    private static final String INDEX = Constant.INDEX;
    private static final String TIMESTAMP = Constant.TIMESTAMP;
    private boolean hasTs = false;
    private boolean schemaTsCol = false;
    private long timestamp = -1;
    private boolean hasHeader = Constant.HAS_HEADER;

    public ParseCsvUtil(String filePath, String tableName, List<String[]> scheamInfo) {
        this.filePath = filePath;
        this.tableName = tableName;
        this.scheamInfo = scheamInfo;
    }

    private HashMap<String, Object> read(CsvReader reader) {
        HashMap<String, Object> map = new HashMap<>();
        String columnName;
        String columnType;
        String value = null;
        int columnIndex = 0;
        for (String[] string : scheamInfo) {
            columnName = string[0];
            columnType = string[1];
            if (string.length == 3) {
                columnIndex = Integer.valueOf(string[2]);
            }
            try {
                value = reader.getValues()[columnIndex];
            } catch (IOException e) {
                e.printStackTrace();
            }
            switch (columnType) {
                case "int16":
                    map.put(columnName, Short.parseShort(value));
                    break;
                case "int32":
                    map.put(columnName, Integer.parseInt(value));
                    break;
                case "int64":
                    map.put(columnName, Long.parseLong(value));
                    break;
                case "string":
                    map.put(columnName, value);
                    break;
                case "float":
                    map.put(columnName, Float.parseFloat(value));
                    break;
                case "double":
                    map.put(columnName, Double.parseDouble(value));
                    break;
                case "boolean":
                    map.put(columnName, Boolean.parseBoolean(value));
                    break;
                case "date":
                    map.put(columnName, Date.valueOf(value));
                    break;
                case "timestamp":
                    map.put(columnName, Timestamp.valueOf(value));
                    break;
                default:
            }
            if (!hasTs && InitClient.contains(";", TIMESTAMP, columnName)) {
                hasTs = true;
            }
            if (hasTs && !schemaTsCol) {
                if (columnName.equals(TIMESTAMP.trim())) {
                    if (columnType.equals("string") || columnType.equals("int64")) {
                        timestamp = Long.parseLong(value);
                    } else if (columnType.equals("timestamp")) {
                        timestamp = Timestamp.valueOf(value).getTime();
                    } else {
                        logger.error("incorrect format for timestamp!");
                        throw new RuntimeException("incorrect format for timestamp!");
                    }
                }
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
            List<ColumnDesc> schemaOfRtidb = InitClient.getSchemaOfRtidb(tableName);
            for (ColumnDesc columnDesc : schemaOfRtidb) {
                if (columnDesc.getIsTsCol()) {
                    schemaTsCol = true;
                }
            }
            reader = new CsvReader(filePath, Constant.CSV_SEPARATOR.toCharArray()[0], Charset.forName(Constant.CSV_ENCODINGFORMAT));
            if (hasHeader) {
                reader.readHeaders();
            }
            int clientIndex = 0;
            while (reader.readRecord()) {
                logger.debug("read data：{}", reader.getRawRecord());
                HashMap<String, Object> map = read(reader);
                if (clientIndex == InitClient.MAX_THREAD_NUM) {
                    clientIndex = 0;
                }
                TableSyncClient client = InitClient.getTableSyncClient()[clientIndex];
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), hasTs, timestamp, client, tableName, map));
                clientIndex++;
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
        List<String[]> result = new ArrayList<>();
        for (String string : lines) {
            if (string == null || string.trim().equals("")) continue;
            String[] strs = string.split(";");
            for (int i = 0; i < strs.length; i++) {
                strs[i] = strs[i].split("=")[1].trim();
            }
            result.add(strs);
        }
        return result;
    }

    public static List<ColumnDesc> getSchemaOfRtidb(List<String[]> schemaList) {
        List<ColumnDesc> list = new ArrayList<>();
        String columnName;
        String type;
        ColumnDesc columnDesc;
        for (String[] string : schemaList) {
            ColumnDesc.Builder builder = ColumnDesc.newBuilder();
            columnName = string[0];
            type = string[1];
            builder.setName(columnName);
            if (InitClient.contains(";", INDEX, columnName)) {
                builder.setAddTsIdx(true);
            }
            if (InitClient.contains(";", TIMESTAMP, columnName)) {
                builder.setIsTsCol(true);
            }
            switch (type) {
                case "int16":
                    builder.setType(InitClient.stringOf(ColumnType.kInt16));
                    break;
                case "int32":
                    builder.setType(InitClient.stringOf(ColumnType.kInt32));
                    break;
                case "int64":
                    builder.setType(InitClient.stringOf(ColumnType.kInt64));
                    break;
                case "string":
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                case "boolean":
                    builder.setType(InitClient.stringOf(ColumnType.kBool));
                    break;
                case "float":
                    builder.setType(InitClient.stringOf(ColumnType.kFloat));
                    break;
                case "double":
                    builder.setType(InitClient.stringOf(ColumnType.kDouble));
                    break;
                case "date":
                    builder.setType(InitClient.stringOf(ColumnType.kDate));
                    break;
                case "timestamp":
                    builder.setType(InitClient.stringOf(ColumnType.kTimestamp));
                    break;
                default:
            }
            columnDesc = builder.build();
            list.add(columnDesc);
        }
        return list;
    }
}
