package com._4paradigm.dataimporter.parseutil;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.util.Strings;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ParseParquetUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseParquetUtil.class);

    private String filePath;
    private String tableName;
    private MessageType schema;
    private static final String INDEX = Constant.INDEX;
    private static final String TIMESTAMP = Constant.TIMESTAMP;
    private boolean hasTs = false;
    private boolean schemaTsCol;
    private long timestamp = -1;
    private static final String INPUT_COLUMN_INDEX = Strings.isBlank(Constant.INPUT_COLUMN_INDEX) ? null : Constant.INPUT_COLUMN_INDEX;
    private static int[] arr = StringUtils.isBlank(INPUT_COLUMN_INDEX)
            ? null
            : new int[INPUT_COLUMN_INDEX.split(",").length];
    private final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long ONE_DAY_IN_MILLIS = 86400000;

    static {
        if (arr != null) {
            String[] sarr = INPUT_COLUMN_INDEX.split(",");
            for (int i = 0; i < sarr.length; i++) {
                arr[i] = Integer.parseInt(sarr[i].trim());
            }
        }
    }

    public ParseParquetUtil(String filePath, String tableName, MessageType schema) {
        this.filePath = filePath;
        this.tableName = tableName;
        this.schema = schema;
        this.schemaTsCol = InitClient.hasTsCol(tableName);
    }

    private HashMap<String, Object> read(SimpleGroup group) {
        HashMap<String, Object> map = new HashMap<>();
        String columnName;
        PrimitiveType.PrimitiveTypeName columnType;
        int index;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (arr == null) {
                index = i;
            } else {
                index = arr[i];
            }
            Type type = schema.getType(i);
            columnName = schema.getFieldName(i);
            columnType = type.asPrimitiveType().getPrimitiveTypeName();
            try {
                switch (columnType) {
                    case INT32:
                        if (type.asPrimitiveType().getOriginalType() != null &&
                                "date".equalsIgnoreCase(type.asPrimitiveType().getOriginalType().name())) {
                            int offsetDays = group.getInteger(index, 0);
                            Date date = new Date(offsetDays * ONE_DAY_IN_MILLIS);
                            map.put(columnName, date);
                        } else {
                            map.put(columnName, group.getInteger(index, 0));
                        }
                        break;
                    case INT64:
                        map.put(columnName, group.getLong(index, 0));
                        break;
                    case INT96:
                        Binary binary = group.getInt96(index, 0);
                        map.put(columnName, new DateTime(getTimestamp(binary)));
                        break;
                    case FLOAT:
                        map.put(columnName, group.getFloat(index, 0));
                        break;
                    case DOUBLE:
                        map.put(columnName, group.getDouble(index, 0));
                        break;
                    case BOOLEAN:
                        map.put(columnName, group.getBoolean(index, 0));
                        break;
                    case BINARY:
                        map.put(columnName, new String(group.getBinary(index, 0).getBytes()));
                        break;
                    case FIXED_LEN_BYTE_ARRAY:
                        map.put(columnName, new String(group.getBinary(index, 0).getBytes()));
                        break;
                    default:

                }
            } catch (Exception e) {
                if (InitClient.contains(";", TIMESTAMP, columnName)) {
                    logger.error("ts column is null");
                    return new HashMap<>();
                }
                map.put(columnName, null);
            }
            if (!hasTs && InitClient.contains(";", TIMESTAMP, columnName)) {
                hasTs = true;
            }
            if (hasTs && !schemaTsCol) {
                if (columnName.equals(TIMESTAMP.trim())) {
                    if (columnType.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                        timestamp = group.getLong(index, 0);
                    } else if(columnType.equals(PrimitiveType.PrimitiveTypeName.INT96)) {
                        timestamp = getTimestamp(group.getInt96(index, 0));
                    }
                    else {
                        logger.error("incorrect format for timestamp!");
                        throw new RuntimeException("incorrect format for timestamp!");
                    }

                }
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
                if (map.size() == 0) {
                    continue;
                }
                if (clientIndex == InitClient.MAX_THREAD_NUM) {
                    clientIndex = 0;
                }
                TableSyncClient client = InitClient.getTableSyncClient()[clientIndex];
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(taskId.getAndIncrement()), hasTs, timestamp, client, tableName, map));
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
        MessageType schema = readFooter.getFileMetaData().getSchema();
        if (INPUT_COLUMN_INDEX == null) {
            return schema;
        }
        List<Type> typeList = new ArrayList<>();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (InitClient.contains(",", INPUT_COLUMN_INDEX, String.valueOf(i))) {
                typeList.add(schema.getType(i));
            }
        }
        return new MessageType("parquet", typeList);
    }

    public static List<ColumnDesc> getSchemaOfRtidb(MessageType schema) {
        List<ColumnDesc> list = new ArrayList<>();
        ColumnDesc columnDesc;
        String columnName;
        PrimitiveType.PrimitiveTypeName columnType;
        for (int i = 0; i < schema.getFieldCount(); i++) {
            ColumnDesc.Builder builder = ColumnDesc.newBuilder();
            columnName = schema.getFieldName(i);
            columnType = schema.getType(i).asPrimitiveType().getPrimitiveTypeName();
            builder.setName(columnName);
            if (InitClient.contains(";", INDEX, columnName)) {
                builder.setAddTsIdx(true);
            }
            if (InitClient.contains(";", TIMESTAMP, columnName)) {
                builder.setIsTsCol(true);
            }
            switch (columnType) {
                case INT32:
                    builder.setType(InitClient.stringOf(ColumnType.kInt32));
                    break;
                case INT64:
                    builder.setType(InitClient.stringOf(ColumnType.kInt64));
                    break;
                case INT96:
                    builder.setType(InitClient.stringOf(ColumnType.kTimestamp));
                    break;
                case BINARY:
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                case BOOLEAN:
                    builder.setType(InitClient.stringOf(ColumnType.kBool));
                    break;
                case FLOAT:
                    builder.setType(InitClient.stringOf(ColumnType.kFloat));
                    break;
                case DOUBLE:
                    builder.setType(InitClient.stringOf(ColumnType.kDouble));
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    builder.setType(InitClient.stringOf(ColumnType.kString));
                    break;
                default:
            }
            columnDesc = builder.build();
            list.add(columnDesc);
        }
        return list;
    }

    private long getTimestamp(Binary binary) {
        if (binary.length() != 12) {
            throw new RuntimeException("Parquet timestamp must be 12 bytes, actual " + binary.length());
        }
        byte[] bytes = binary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private long julianDayToMillis(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }
}

