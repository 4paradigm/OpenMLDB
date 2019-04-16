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
    private Long timestamp = null;
    private static final String INPUT_COLUMN_INDEX = Strings.isBlank(Constant.INPUT_COLUMN_INDEX) ? null : Constant.INPUT_COLUMN_INDEX;

    public ParseOrcUtil(String path, String tableName, TypeDescription schema) {
        this.path = path;
        this.tableName = tableName;
        this.schema = schema;
    }

    public HashMap<String, Object> read(StructObjectInspector inspector, Object row) {
        HashMap<String, Object> map = new HashMap<>();
        List<Object> valueList = inspector.getStructFieldsDataAsList(row);
        logger.debug("row:{}", valueList);
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            String columnName = schema.getFieldNames().get(i);
            String columnType = schema.getChildren().get(i).toString();
            Object value = valueList.get(i);
            if (columnType.equalsIgnoreCase("binary")) {
                map.put(columnName, new String(((BytesWritable) value).getBytes()));
            } else if (columnType.equalsIgnoreCase("boolean")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                if (s.equals("0")) {
                    map.put(columnName, false);
                } else {
                    map.put(columnName, true);
                }
            } else if (columnType.equalsIgnoreCase("tinyint")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                map.put(columnName, Short.valueOf(s));
            } else if (columnType.equalsIgnoreCase("date")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                map.put(columnName, Long.valueOf(s));
            } else if (columnType.equalsIgnoreCase("double")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                map.put(columnName, Double.valueOf(s));
            } else if (columnType.equalsIgnoreCase("float")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                map.put(columnName, Float.valueOf(s));
            } else if (columnType.equalsIgnoreCase("int")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                map.put(columnName, Integer.valueOf(s));
            } else if (columnType.equalsIgnoreCase("bigint")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                map.put(columnName, Long.valueOf(s));
            } else if (columnType.equalsIgnoreCase("smallint")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                map.put(columnName, Short.valueOf(s));
            } else if (columnType.equalsIgnoreCase("string")) {
                map.put(columnName, String.valueOf(value));
            } else if (columnType.equalsIgnoreCase("timestamp")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0000-00-00 00:00:00" : String.valueOf(value);
                map.put(columnName, Timestamp.valueOf(s));
            } else if (columnType.substring(0, 4).equalsIgnoreCase("char")) {
                map.put(columnName, String.valueOf(value));
            } else if (columnType.substring(0, 7).equalsIgnoreCase("varchar")) {
                map.put(columnName, String.valueOf(value));
            } else if (columnType.substring(0, 7).equalsIgnoreCase("decimal")) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                map.put(columnName, Double.valueOf(s));
            }
            if (i == TIMESTAMP_INDEX) {
                String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
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
        if (reader != null) {
            return reader.getSchema();
        } else {
            return null;
        }
    }

    public static List<ColumnDesc> getSchemaOfRtidb(TypeDescription schema) {
        List<ColumnDesc> list = new ArrayList<>();
        String columnName;
        String type;
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            if (INPUT_COLUMN_INDEX == null || INPUT_COLUMN_INDEX.contains(String.valueOf(i))) {
                ColumnDesc columnDesc = new ColumnDesc();
                columnName = schema.getFieldNames().get(i);
                type = schema.getChildren().get(i).toString();
                columnDesc.setName(columnName);
                if (INDEX.contains(columnName)) {
                    columnDesc.setAddTsIndex(true);
                }
                if (type.equalsIgnoreCase("binary")) {
                    columnDesc.setType(ColumnType.kString);
                } else if (type.equalsIgnoreCase("boolean")) {
                    columnDesc.setType(ColumnType.kBool);
                } else if (type.equalsIgnoreCase("tinyint")) {
                    columnDesc.setType(ColumnType.kInt16);
                } else if (type.equalsIgnoreCase("date")) {
                    columnDesc.setType(ColumnType.kInt64);
                } else if (type.equalsIgnoreCase("double")) {
                    columnDesc.setType(ColumnType.kDouble);
                } else if (type.equalsIgnoreCase("float")) {
                    columnDesc.setType(ColumnType.kFloat);
                } else if (type.equalsIgnoreCase("int")) {
                    columnDesc.setType(ColumnType.kInt32);
                } else if (type.equalsIgnoreCase("bigint")) {
                    columnDesc.setType(ColumnType.kInt64);
                } else if (type.equalsIgnoreCase("smallint")) {
                    columnDesc.setType(ColumnType.kInt16);
                } else if (type.equalsIgnoreCase("string")) {
                    columnDesc.setType(ColumnType.kString);
                } else if (type.equalsIgnoreCase("timestamp")) {
                    columnDesc.setType(ColumnType.kTimestamp);
                } else if (type.substring(0, 4).equalsIgnoreCase("char")) {
                    columnDesc.setType(ColumnType.kString);
                } else if (type.substring(0, 7).equalsIgnoreCase("varchar")) {
                    columnDesc.setType(ColumnType.kString);
                } else if (type.substring(0, 7).equalsIgnoreCase("decimal")) {
                    columnDesc.setType(ColumnType.kDouble);
                }
                list.add(columnDesc);
            }
        }
        return list;
    }

}
