package com._4paradigm.dataimporter.parseutil;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.io.DateWritable;
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
    private static final String TIMESTAMP = Constant.TIMESTAMP;
    private boolean hasTs = false;
    private boolean schemaTsCol = false;
    private long timestamp = -1;
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
        int columnIndex;
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            if (arr == null) {
                columnIndex = i;
            } else {
                columnIndex = arr[i];
            }
            String columnName = schema.getFieldNames().get(i);
            TypeDescription.Category columnType = schema.getChildren().get(i).getCategory();
            Object value = valueList.get(columnIndex);
            String s;
            switch (columnType) {
                case BINARY:
                    map.put(columnName, new String(((BytesWritable) value).copyBytes()));
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
                    map.put(columnName, ((DateWritable) value).get());
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
            if (!hasTs && InitClient.contains(";", TIMESTAMP, columnName)) {
                hasTs = true;
            }
            if (hasTs && !schemaTsCol) {
                if (columnName.equals(TIMESTAMP.trim())) {
                    if (columnType.equals(TypeDescription.Category.STRING) || columnType.equals(TypeDescription.Category.LONG)) {
                        s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        timestamp = Long.valueOf(s);
                    } else if (columnType.equals(TypeDescription.Category.TIMESTAMP)) {
                        s = StringUtils.isBlank(String.valueOf(value)) ? "0000-00-00 00:00:00" : String.valueOf(value);
                        timestamp = Timestamp.valueOf(s).getTime();
                    } else {
                        logger.error("incorrect format for timestamp!");
                        throw new RuntimeException("incorrect format for timestamp!");
                    }
                }
            }
        }
        return map;
    }

    public void put() {
        try {
            List<ColumnDesc> schemaOfRtidb = InitClient.getSchemaOfRtidb(tableName);
            for (ColumnDesc columnDesc : schemaOfRtidb) {
                if (columnDesc.getIsTsCol()) {
                    schemaTsCol = true;
                }
            }
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
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), hasTs, timestamp, client, tableName, map));
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
            if (InitClient.contains(",", INPUT_COLUMN_INDEX, String.valueOf(i))) {
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
            ColumnDesc.Builder builder = ColumnDesc.newBuilder();
            columnName = schema.getFieldNames().get(i);
            columnType = schema.getChildren().get(i).getCategory();
            builder.setName(columnName);
            if (InitClient.contains(";", INDEX, columnName)) {
                builder.setAddTsIdx(true);
            }
            if (InitClient.contains(";", TIMESTAMP, columnName)) {
                builder.setIsTsCol(true);
            }
            switch (columnType) {
                case BINARY:
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                case BOOLEAN:
                    builder.setType(InitClient.stringOf(ColumnType.kBool));
                    break;
                case BYTE:
                    builder.setType(InitClient.stringOf(ColumnType.kInt16));
                    break;
                case DATE:
                    builder.setType(InitClient.stringOf(ColumnType.kDate));
                    break;
                case DOUBLE:
                    builder.setType(InitClient.stringOf(ColumnType.kDouble));
                    break;
                case FLOAT:
                    builder.setType(InitClient.stringOf(ColumnType.kFloat));
                    break;
                case INT:
                    builder.setType(InitClient.stringOf(ColumnType.kInt32));
                    break;
                case LONG:
                    builder.setType(InitClient.stringOf(ColumnType.kInt64));
                    break;
                case SHORT:
                    builder.setType(InitClient.stringOf(ColumnType.kInt16));
                    break;
                case STRING:
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                case TIMESTAMP:
                    builder.setType(InitClient.stringOf(ColumnType.kTimestamp));
                    break;
                case CHAR:
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                case VARCHAR:
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                case DECIMAL:
                    builder.setType(InitClient.stringOf(ColumnType.kDouble));
                    break;
                default:
            }
            ColumnDesc columnDesc = builder.build();
            list.add(columnDesc);
        }
        return list;
    }

}
