package com._4paradigm.dataimporter.parseutil;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.logging.log4j.util.Strings;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ParseOrcUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseOrcUtil.class);
    private String path;
    private String tableName;
    private TypeDescription schema;
    private static final String INDEX = Constant.INDEX;
    private final int TIMESTAMP_INDEX = Integer.parseInt(StringUtils.isBlank(Constant.TIMESTAMP_INDEX) ? "-1" : Constant.TIMESTAMP_INDEX);
    private long timestamp;
    private static final String INPUT_COLUMN_INDEX = Strings.isBlank(Constant.INPUT_COLUMN_INDEX) ? null : Constant.INPUT_COLUMN_INDEX;
    private static int[] arr = StringUtils.isBlank(INPUT_COLUMN_INDEX)
            ? null
            : new int[INPUT_COLUMN_INDEX.split(",").length];

    static {
        if (arr != null) {
            String[] sarr = INPUT_COLUMN_INDEX.split(",");
            for (int i = 0; i < sarr.length; i++) {
                arr[i] = Integer.parseInt(sarr[i].trim());
            }
        }
    }

    public ParseOrcUtil(String path, String tableName, TypeDescription schema) {
        this.path = path;
        this.tableName = tableName;
        this.schema = schema;
    }

    public HashMap<String, Object> read(StructObjectInspector inspector, Object row) {
        HashMap<String, Object> map = new HashMap<>();
        List<Object> valueList = inspector.getStructFieldsDataAsList(row);
        logger.debug("row:{}", valueList);
        int index;
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            if (arr == null) {
                index = i;
            } else {
                index = arr[i];
            }
            String columnName = schema.getFieldNames().get(i);
            TypeDescription.Category columnType = schema.getChildren().get(i).getCategory();
            Object value = valueList.get(index);
            String s;
            switch (columnType) {
                case BINARY:
                    map.put(columnName, new String(((BytesWritable) value).getBytes()));
                    break;
                case BOOLEAN:
                    s = StringUtils.isBlank(String.valueOf(value)) ? null : String.valueOf(value);
                    if (s == null) {
                        map.put(columnName, false);
                    } else {
                        map.put(columnName, Boolean.parseBoolean(s));
                    }
                    break;
                case BYTE:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Short.valueOf(s));
                    break;
                case DATE:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Long.valueOf(s));
                    break;
                case DOUBLE:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                    map.put(columnName, Double.valueOf(s));
                    break;
                case FLOAT:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                    map.put(columnName, Float.valueOf(s));
                    break;
                case INT:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Integer.valueOf(s));
                    break;
                case LONG:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Long.valueOf(s));
                    break;
                case SHORT:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Short.valueOf(s));
                    break;
                case STRING:
                    map.put(columnName, String.valueOf(value));
                    break;
                case TIMESTAMP:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0000-00-00 00:00:00" : String.valueOf(value);
                    map.put(columnName, Timestamp.valueOf(s));
                    break;
                case CHAR:
                    map.put(columnName, String.valueOf(value));
                    break;
                case VARCHAR:
                    map.put(columnName, String.valueOf(value));
                    break;
                case DECIMAL:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                    map.put(columnName, Double.valueOf(s));
                    break;
                default:
            }
            if (index == TIMESTAMP_INDEX) {
                s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                timestamp = Long.valueOf(s);
            }
        }
        return map;
    }

    public void put() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.framework.name", "local");
            conf.set("fs.defaultFS", "file:///");
            Reader reader = OrcFile.createReader(
                    new Path(path),
                    OrcFile.readerOptions(conf));
            RecordReader records = reader.rows();
            Object row = null;
            StructObjectInspector inspector
                    = (StructObjectInspector) reader.getObjectInspector();
            AtomicLong id = new AtomicLong(1);//task的id
            int clientIndex = 0;//用于选择使用哪个客户端执行put操作
            while (records.hasNext()) {
                row = records.next(row);
                HashMap<String, Object> map = read(inspector, row);
                if (clientIndex == InitClient.MAX_THREAD_NUM) {
                    clientIndex = 0;
                }
                TableSyncClient client = InitClient.getTableSyncClient()[clientIndex];
                if (TIMESTAMP_INDEX == -1) {
                    timestamp = System.currentTimeMillis();
                }
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), timestamp, client, tableName, map));
                clientIndex++;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static TypeDescription getSchema(String path) {
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(
                    new Path(path),
                    OrcFile.readerOptions(conf));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (reader == null) {
            return null;
        }
        TypeDescription schema = reader.getSchema();
        if (INPUT_COLUMN_INDEX == null) {
            return schema;
        }
        TypeDescription result = TypeDescription.createStruct();
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            if (INPUT_COLUMN_INDEX.contains(String.valueOf(i))) {
                result.addField(schema.getFieldNames().get(i), schema.getChildren().get(i));
            }
        }
        return result;

    }

    public static List<ColumnDesc> getSchemaOfRtidb(TypeDescription schema) {
        List<ColumnDesc> list = new ArrayList<>();
        String columnName;
        TypeDescription.Category columnType;
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            ColumnDesc columnDesc = new ColumnDesc();
            columnName = schema.getFieldNames().get(i);
            columnType = schema.getChildren().get(i).getCategory();
            columnDesc.setName(columnName);
            if (INDEX.contains(columnName)) {
                columnDesc.setAddTsIndex(true);
            }
            switch (columnType) {
                case BINARY:
                    columnDesc.setType(ColumnType.kString);
                    break;
                case BOOLEAN:
                    columnDesc.setType(ColumnType.kBool);
                    break;
                case BYTE:
                    columnDesc.setType(ColumnType.kInt16);
                    break;
                case DATE:
                    columnDesc.setType(ColumnType.kInt64);
                    break;
                case DOUBLE:
                    columnDesc.setType(ColumnType.kDouble);
                    break;
                case FLOAT:
                    columnDesc.setType(ColumnType.kFloat);
                    break;
                case INT:
                    columnDesc.setType(ColumnType.kInt32);
                    break;
                case LONG:
                    columnDesc.setType(ColumnType.kInt64);
                    break;
                case SHORT:
                    columnDesc.setType(ColumnType.kInt16);
                    break;
                case STRING:
                    columnDesc.setType(ColumnType.kString);
                    break;
                case TIMESTAMP:
                    columnDesc.setType(ColumnType.kTimestamp);
                    break;
                case CHAR:
                    columnDesc.setType(ColumnType.kString);
                    break;
                case VARCHAR:
                    columnDesc.setType(ColumnType.kString);
                    break;
                case DECIMAL:
                    columnDesc.setType(ColumnType.kDouble);
                    break;
                default:
            }
            list.add(columnDesc);
        }
        return list;
    }

}
