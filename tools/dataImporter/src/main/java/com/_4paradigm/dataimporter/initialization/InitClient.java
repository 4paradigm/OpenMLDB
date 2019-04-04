package com._4paradigm.dataimporter.initialization;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.ns.NS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InitClient {
    private static Logger logger = LoggerFactory.getLogger(InitClient.class);

    private static final String ZKENDPOINTS = Constant.ZKENDPOINTS;
    private static final String ZKROOTPATH = Constant.ZKROOTPATH;
    // 下面这几行变量定义不需要改
    // NameServerClientImpl要么做成单例, 要么用完之后就调用close, 否则会导致fd泄露
    private static NameServerClientImpl nsc = new NameServerClientImpl(ZKENDPOINTS, ZKROOTPATH + "/leader");
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient clusterClient = null;
    public static final int CLIENTCOUNT = Constant.CLIENTCOUNT;
    private static TableSyncClient[] tableSyncClient = new TableSyncClient[CLIENTCOUNT];

    /**
     * 初始化客户端
     */
    public static void initClient() {
        try {
            nsc.init();
            config.setZkEndpoints(ZKENDPOINTS);
            config.setZkRootPath(ZKROOTPATH);
            // 设置读策略. 默认是读leader
            // 可以按照表级别来设置
            // Map<String, ReadStrategy> strategy = new HashMap<String, ReadStrategy>();
            // strategy put传入的参数是表名和读策略. 读策略可以设置为读leader(kReadLeader)或者读本地(kReadLocal).
            // 读本地的策略是客户端优先选择读取部署在当前机器的节点, 如果当前机器没有部署tablet则随机选择一个从节点读取, 如果没有从节点就读主
            // strategy.put("test1", ReadStrategy.kReadLocal);
            // config.setReadStrategies(strategy);
            // 如果要过滤掉同一个pk下面相同ts的值添加如下设置
            // config.setRemoveDuplicateByTime(true);
            // 设置最大重试次数
            // config.setMaxRetryCnt(3);
            //初始化最大线程个数的client
            for (int i = 0; i < CLIENTCOUNT; i++) {
                clusterClient = new RTIDBClusterClient(config);
                clusterClient.init();
                tableSyncClient[i] = new TableSyncClientImpl(clusterClient);
            }
//            tableSyncClient = new TableSyncClientImpl(clusterClient);
//            tableAsyncClient = new TableAsyncClientImpl(clusterClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createSchemaTable(String tableName, List<ColumnDesc> schemaList) {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(tableName)  // 设置表名
                .setReplicaNum(1)    // 设置副本数. 此设置是可选的, 默认为3
                .setPartitionNum(1)  // 设置分片数. 此设置是可选的, 默认为16
                //.setCompressType(NS.CompressType.kSnappy) // 设置数据压缩类型. 此设置是可选的默认为不压缩
                //.setTtlType("kLatestTime")  // 设置ttl类型. 此设置是可选的, 默认为"kAbsoluteTime"按时间过期
                .setTtl(0);      // 设置ttl. 如果ttl类型是kAbsoluteTime, 那么ttl的单位是分钟.
        for (ColumnDesc schema : schemaList) {
            builder.addColumnDesc(NS.ColumnDesc.newBuilder()
                    .setType(stringOf(schema.getType()))
                    .setName(schema.getName())
                    .setAddTsIdx(schema.isAddTsIndex()));
        }
        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        if (ok) {
            logger.info("the RrtidbSchemaTable is created ：" + ok);
        } else {
            logger.error("the RrtidbSchemaTable is created ：" + ok);
        }
        clusterClient.refreshRouteTable();
    }

    /**
     * ColumnType转换为对应的String类型
     *
     * @param columnType
     * @return
     */
    public static String stringOf(ColumnType columnType) {
        if (ColumnType.kString.equals(columnType)) {
            return "string";
        } else if (ColumnType.kInt16.equals(columnType)) {
            return "int16";
        } else if (ColumnType.kInt32.equals(columnType)) {
            return "int32";
        } else if (ColumnType.kInt64.equals(columnType)) {
            return "int64";
        } else if (ColumnType.kUInt16.equals(columnType)) {
            return "uint16";
        } else if (ColumnType.kUInt32.equals(columnType)) {
            return "uint32";
        } else if (ColumnType.kUInt64.equals(columnType)) {
            return "uint64";
        } else if (ColumnType.kFloat.equals(columnType)) {
            return "float";
        } else if (ColumnType.kDouble.equals(columnType)) {
            return "double";
        } else if (ColumnType.kTimestamp.equals(columnType)) {
            return "timestamp";
        } else if (ColumnType.kDate.equals(columnType)) {
            return "date";
        } else if (ColumnType.kBool.equals(columnType)) {
            return "bool";
        } else if (ColumnType.kEmptyString.equals(columnType)) {
            return "emptyString";
        } else {
            throw new RuntimeException("not supported type with " + columnType);
        }
    }


    /**
     * @param path
     * @return
     * @description 读取parquet文件获取schema
     */
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


    /**
     * @param string
     * @return
     * @description 得到基本数据类型的字符串
     */
    public static String getDataType(String string) {
        String[] array = string.split(" ");
        return array[1];
    }

    /**
     * for parquet
     *
     * @param schema
     * @return
     */
    public static List<ColumnDesc> getSchemaOfRtidb(MessageType schema) {
        List<ColumnDesc> list = new ArrayList<>();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            ColumnDesc columnDesc = new ColumnDesc();
            if (Constant.PARQUET_INDEX.contains(schema.getFieldName(i))) {
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

    /**
     * for csv
     *
     * @param schemaList
     * @return
     */
    public static List<ColumnDesc> getSchemaOfRtidb(List<String[]> schemaList) {
        List<ColumnDesc> list = new ArrayList<>();
        String columnName;
        String type;
        for (String[] string : schemaList) {
            ColumnDesc columnDesc = new ColumnDesc();
            if (Constant.CSV_INDEX.contains(string[0].split("=")[1])) {
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
            }
            columnDesc.setName(columnName);
            list.add(columnDesc);
        }
        return list;
    }

    public static List<ColumnDesc> getRtidbSchema2() {
        List<ColumnDesc> list = new ArrayList<>();
        ColumnDesc columnDesc = new ColumnDesc();
        columnDesc.setType(ColumnType.kString);
        columnDesc.setName("binary_1");
        columnDesc.setAddTsIndex(false);
        list.add(columnDesc);

        ColumnDesc columnDesc_1 = new ColumnDesc();
        columnDesc_1.setType(ColumnType.kBool);
        columnDesc_1.setName("boolean_1");
        columnDesc_1.setAddTsIndex(false);
        list.add(columnDesc_1);

        ColumnDesc columnDesc_2 = new ColumnDesc();
        columnDesc_2.setType(ColumnType.kInt16);
        columnDesc_2.setName("byte_1");
        columnDesc_2.setAddTsIndex(false);
        list.add(columnDesc_2);

        ColumnDesc columnDesc_3 = new ColumnDesc();
        columnDesc_3.setType(ColumnType.kInt64);
        columnDesc_3.setName("date_1");
        columnDesc_3.setAddTsIndex(false);
        list.add(columnDesc_3);

        ColumnDesc columnDesc_4 = new ColumnDesc();
        columnDesc_4.setType(ColumnType.kDouble);
        columnDesc_4.setName("double_1");
        columnDesc_4.setAddTsIndex(false);
        list.add(columnDesc_4);

        ColumnDesc columnDesc_5 = new ColumnDesc();
        columnDesc_5.setType(ColumnType.kFloat);
        columnDesc_5.setName("float_1");
        columnDesc_5.setAddTsIndex(false);
        list.add(columnDesc_5);

        ColumnDesc columnDesc_6 = new ColumnDesc();
        columnDesc_6.setType(ColumnType.kInt32);
        columnDesc_6.setName("int_1");
        columnDesc_6.setAddTsIndex(true);
        list.add(columnDesc_6);

        ColumnDesc columnDesc_7 = new ColumnDesc();
        columnDesc_7.setType(ColumnType.kInt64);
        columnDesc_7.setName("long_1");
        columnDesc_7.setAddTsIndex(false);
        list.add(columnDesc_7);

        ColumnDesc columnDesc_8 = new ColumnDesc();
        columnDesc_8.setType(ColumnType.kInt16);
        columnDesc_8.setName("short_1");
        columnDesc_8.setAddTsIndex(false);
        list.add(columnDesc_8);

        ColumnDesc columnDesc_9 = new ColumnDesc();
        columnDesc_9.setType(ColumnType.kString);
        columnDesc_9.setName("string_1");
        columnDesc_9.setAddTsIndex(false);
        list.add(columnDesc_9);

        ColumnDesc columnDesc_10 = new ColumnDesc();
        columnDesc_10.setType(ColumnType.kTimestamp);
        columnDesc_10.setName("timestamp_1");
        columnDesc_10.setAddTsIndex(false);
        list.add(columnDesc_10);

        ColumnDesc columnDesc_11 = new ColumnDesc();
        columnDesc_11.setType(ColumnType.kString);
        columnDesc_11.setName("char_1");
        columnDesc_11.setAddTsIndex(false);
        list.add(columnDesc_11);

        ColumnDesc columnDesc_12 = new ColumnDesc();
        columnDesc_12.setType(ColumnType.kString);
        columnDesc_12.setName("varchar_1");
        columnDesc_12.setAddTsIndex(false);
        list.add(columnDesc_12);

        ColumnDesc columnDesc_13 = new ColumnDesc();
        columnDesc_13.setType(ColumnType.kDouble);
        columnDesc_13.setName("decimal_1");
        columnDesc_13.setAddTsIndex(false);
        list.add(columnDesc_13);

        return list;
    }

    /**
     * 删除表
     *
     * @param tableName
     * @return
     */
    public static boolean dropTable(String tableName) {
        return nsc.dropTable(tableName);
    }

    public static TableSyncClient[] getTableSyncClient() {
        return tableSyncClient;
    }

}
