package com._4paradigm.dataimporter.operator;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.ns.NS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OperateTable {
    private static Logger logger = LoggerFactory.getLogger(OperateTable.class);

    private static String zkEndpoints = "172.27.128.37:7181,172.27.128.37:7182,172.27.128.37:7183";  // 配置zk地址, 和集群启动配置中的zk_cluster保持一致
    private static String zkRootPath = "/rtidb_cluster";   // 配置集群的zk根路径, 和集群启动配置中的zk_root_path保持一致
    // 下面这几行变量定义不需要改
    private static String leaderPath = zkRootPath + "/leader";
    // NameServerClientImpl要么做成单例, 要么用完之后就调用close, 否则会导致fd泄露
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient clusterClient = null;
    public static final int SUM =7;
    private static TableSyncClient[] tableSyncClient = new TableSyncClient[SUM];

    /**
     * 初始化客户端
     */
    public static void initClient(){
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkRootPath(zkRootPath);
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
            clusterClient = new RTIDBClusterClient(config);
            clusterClient.init();
            //初始化最大线程个数的client
            for(int i = 0; i< SUM; i++){
                tableSyncClient[i]=new TableSyncClientImpl(clusterClient);
            }
//            tableSyncClient = new TableSyncClientImpl(clusterClient);
//            tableAsyncClient = new TableAsyncClientImpl(clusterClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 创建有schema的表
     */
    public static void createSchemaTable(String tableName, List<ColumnDesc> schemaList) {
        NS.TableInfo.Builder builder =  NS.TableInfo.newBuilder()
                .setName(tableName)  // 设置表名
                .setReplicaNum(1)    // 设置副本数. 此设置是可选的, 默认为3
                .setPartitionNum(1)  // 设置分片数. 此设置是可选的, 默认为16
                //.setCompressType(NS.CompressType.kSnappy) // 设置数据压缩类型. 此设置是可选的默认为不压缩
                //.setTtlType("kLatestTime")  // 设置ttl类型. 此设置是可选的, 默认为"kAbsoluteTime"按时间过期
                .setTtl(0);      // 设置ttl. 如果ttl类型是kAbsoluteTime, 那么ttl的单位是分钟.
        for(ColumnDesc schema:schemaList){
            builder.addColumnDesc(NS.ColumnDesc.newBuilder()
                    .setType(stringOf(schema.getType()))
                    .setName(schema.getName())
                    .setAddTsIdx(schema.isAddTsIndex()));
        }
        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        logger.info("the schema is created ：" + ok);
        clusterClient.refreshRouteTable();
    }

    /**
     * ColumnType转换为对应的String类型
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
        }else if (ColumnType.kUInt32.equals(columnType)) {
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
        }  else if (ColumnType.kBool.equals(columnType)) {
            return "bool";
        } else if (ColumnType.kEmptyString.equals(columnType)) {
            return "emptyString";
        } else {
            throw new RuntimeException("not supported type with " + columnType);
        }
    }

    /**
     * 获得rtidb的schema
     * @return
     */
    public static List<ColumnDesc> getRtidbSchema(){
        List<ColumnDesc> list=new ArrayList<>();
        ColumnDesc columnDesc=new ColumnDesc();
        columnDesc.setType(ColumnType.kInt32);
        columnDesc.setName("int_32");
        columnDesc.setAddTsIndex(true);
        list.add(columnDesc);

        ColumnDesc columnDesc_1=new ColumnDesc();
        columnDesc_1.setType(ColumnType.kInt64);
        columnDesc_1.setName("int_64");
        columnDesc_1.setAddTsIndex(false);
        list.add(columnDesc_1);

        ColumnDesc columnDesc_2=new ColumnDesc();
        columnDesc_2.setType(ColumnType.kString);
        columnDesc_2.setName("int_96");
        columnDesc_2.setAddTsIndex(false);
        list.add(columnDesc_2);

        ColumnDesc columnDesc_3=new ColumnDesc();
        columnDesc_3.setType(ColumnType.kFloat);
        columnDesc_3.setName("float_1");
        columnDesc_3.setAddTsIndex(false);
        list.add(columnDesc_3);

        ColumnDesc columnDesc_4=new ColumnDesc();
        columnDesc_4.setType(ColumnType.kDouble);
        columnDesc_4.setName("double_1");
        columnDesc_4.setAddTsIndex(false);
        list.add(columnDesc_4);

        ColumnDesc columnDesc_5=new ColumnDesc();
        columnDesc_5.setType(ColumnType.kBool);
        columnDesc_5.setName("boolean_1");
        columnDesc_5.setAddTsIndex(false);
        list.add(columnDesc_5);

        ColumnDesc columnDesc_6=new ColumnDesc();
        columnDesc_6.setType(ColumnType.kString);
        columnDesc_6.setName("binary_1");
        columnDesc_6.setAddTsIndex(false);
        list.add(columnDesc_6);

        return list;
    }

    /**
     * 删除表
     * @param tableName
     * @return
     */
    public static boolean dropTable(String tableName){
        return nsc.dropTable(tableName);
    }

    public static TableSyncClient[] getTableSyncClient() {
        return tableSyncClient;
    }

}
