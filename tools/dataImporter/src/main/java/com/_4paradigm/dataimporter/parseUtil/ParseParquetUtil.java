package com._4paradigm.dataimporter.parseUtil;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ParseParquetUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseParquetUtil.class);

    private String filePath;
    private String tableName;
    private MessageType schema;
    private static final String INDEX = Constant.INDEX;
    private final String TIMESTAMP = Constant.TIMESTAMP;
    private Long timestamp = null;

    public ParseParquetUtil(String filePath, String tableName, MessageType schema) {
        this.filePath = filePath;
        this.tableName = tableName;
        this.schema = schema;
    }

    private HashMap<String, Object> read(SimpleGroup group) {
        HashMap<String, Object> map = new HashMap<>();
        String columnName;
        PrimitiveType.PrimitiveTypeName type;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            columnName = schema.getFieldName(i);
            type = schema.getType(i).asPrimitiveType().getPrimitiveTypeName();
            switch (type) {
                case INT32:
                    map.put(columnName, group.getInteger(i, 0));
                    break;
                case INT64:
                    map.put(columnName, group.getLong(i, 0));
                    break;
                case INT96:
                    map.put(columnName, new String(group.getInt96(i, 0).getBytes()));
                    break;
                case FLOAT:
                    map.put(columnName, group.getFloat(i, 0));
                    break;
                case DOUBLE:
                    map.put(columnName, group.getDouble(i, 0));
                    break;
                case BOOLEAN:
                    map.put(columnName, group.getBoolean(i, 0));
                    break;
                case BINARY:
                    map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
                    break;
                default:
            }
            if (columnName.equals(TIMESTAMP)) {
                timestamp = group.getLong(i, 0);
            }
        }
        return map;
    }

    public void put() {
        SimpleGroup group;
        AtomicLong taskId = new AtomicLong(1);
        int clientIndex = 0;
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(filePath));
        try {
            ParquetReader<Group> reader = builder.build();
            while ((group = (SimpleGroup) reader.read()) != null) {
                HashMap<String, Object> map = read(group);
                if (clientIndex == InitClient.MAX_THREAD_NUM) {
                    clientIndex = 0;
                }
                TableSyncClient client = InitClient.getTableSyncClient()[clientIndex];
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(taskId.getAndIncrement()), timestamp, client, tableName, map));
                clientIndex++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static MessageType getSchema(Path path) {
        Configuration configuration = new Configuration();
//         windows 下测试入库impala需要这个配置
//        System.setProperty("hadoop.home.dir",
//                "E:\\mvtech\\software\\hadoop-common-2.2.0-bin-master");
        ParquetMetadata readFooter = null;
        try {
            readFooter = ParquetFileReader.readFooter(configuration,
                    path, ParquetMetadataConverter.NO_FILTER);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return readFooter.getFileMetaData().getSchema();
    }

    public static List<ColumnDesc> getSchemaOfRtidb(MessageType schema) {
        List<ColumnDesc> list = new ArrayList<>();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            ColumnDesc columnDesc = new ColumnDesc();
            if (INDEX.contains(schema.getFieldName(i))) {
                columnDesc.setAddTsIndex(true);
            }
            if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT32)) {
                columnDesc.setType(ColumnType.kInt32);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                columnDesc.setType(ColumnType.kInt64);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96)) {
                columnDesc.setType(ColumnType.kString);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
                columnDesc.setType(ColumnType.kString);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)) {
                columnDesc.setType(ColumnType.kBool);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.FLOAT)) {
                columnDesc.setType(ColumnType.kFloat);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.DOUBLE)) {
                columnDesc.setType(ColumnType.kDouble);
            } else if (schema.getType(i).asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                columnDesc.setType(ColumnType.kString);
            }
            columnDesc.setName(schema.getFieldName(i));
            list.add(columnDesc);
        }
        return list;
    }

}

