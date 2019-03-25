package com.parseParquet;


import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.ns.NS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

/**
 * @描述：解析parseParquet文件
 */
public class ParseParquetUtil {

    private static Logger logger = LoggerFactory.getLogger(ParseParquetUtil.class);
//    private static String path = "file:///Users/innerpeace/Desktop/test.parq";//file://是文件协议，可以不加
    private static String path = "/home/wangbao/test.parq";//file://是文件协议，可以不加
    private static String tableName = "parquetTest";
    private static int sum=7;
    private static int blockingQueueSize=1000;
    private static int corePoolSize=7;
    private static int maximumPoolSiz=7;

    private static String zkEndpoints = "172.27.128.37:7181,172.27.128.37:7182,172.27.128.37:7183";  // 配置zk地址, 和集群启动配置中的zk_cluster保持一致
    private static String zkRootPath = "/rtidb_cluster";   // 配置集群的zk根路径, 和集群启动配置中的zk_root_path保持一致
    // 下面这几行变量定义不需要改
    private static String leaderPath = zkRootPath + "/leader";
    // NameServerClientImpl要么做成单例, 要么用完之后就调用close, 否则会导致fd泄露
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient clusterClient = null;
    private static TableSyncClient[] tableSyncClient = new TableSyncClient[sum];
//     发送同步请求的client
//    private static TableSyncClient tableSyncClient = null;
//    // 发送异步请求的client
//    private static TableAsyncClient tableAsyncClient = null;

    static {
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
            for(int i=0;i<sum;i++){
                tableSyncClient[i]=new TableSyncClientImpl(clusterClient);;
            }
//            tableSyncClient = new TableSyncClientImpl(clusterClient);
//            tableAsyncClient = new TableAsyncClientImpl(clusterClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建有schema的表
    public static void createSchemaTable() {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder = NS.TableInfo.newBuilder()
                .setName(tableName)  // 设置表名
                .setReplicaNum(1)    // 设置副本数. 此设置是可选的, 默认为3
                .setPartitionNum(1)  // 设置分片数. 此设置是可选的, 默认为16
                //.setCompressType(NS.CompressType.kSnappy) // 设置数据压缩类型. 此设置是可选的默认为不压缩
                //.setTtlType("kLatestTime")  // 设置ttl类型. 此设置是可选的, 默认为"kAbsoluteTime"按时间过期
                .setTtl(0);      // 设置ttl. 如果ttl类型是kAbsoluteTime, 那么ttl的单位是分钟.
        // 设置schema信息
        NS.ColumnDesc col0 = NS.ColumnDesc.newBuilder()
                .setName("int_32")    // 设置字段名
                .setAddTsIdx(true)  // 设置是否为index, 如果设置为true表示该字段为维度列, 查询的时候可以通过此列来查询, 否则设置为false
                .setType("int32")  // 设置字段类型, 支持的字段类型有[int32, uint32, int64, uint64, float, double, string]
                .build();
        NS.ColumnDesc col1 = NS.ColumnDesc.newBuilder().setName("int_64").setAddTsIdx(false).setType("int64").build();
        NS.ColumnDesc col2 = NS.ColumnDesc.newBuilder().setName("int_96").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col3 = NS.ColumnDesc.newBuilder().setName("float_1").setAddTsIdx(false).setType("float").build();
        NS.ColumnDesc col4 = NS.ColumnDesc.newBuilder().setName("double_1").setAddTsIdx(false).setType("double").build();
        NS.ColumnDesc col5 = NS.ColumnDesc.newBuilder().setName("boolean_1").setAddTsIdx(false).setType("string").build();
        NS.ColumnDesc col6 = NS.ColumnDesc.newBuilder().setName("binary_1").setAddTsIdx(false).setType("string").build();
        // 将schema添加到builder中
        builder.addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3).addColumnDesc(col4).addColumnDesc(col5).addColumnDesc(col6);
        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        logger.info("the result that the schema is created ：" + ok);
        clusterClient.refreshRouteTable();
    }

    /**
     * @param path parquet文件的绝对路径
     * @throws IOException
     */
    public static void parseParquetWrite(String path) throws IOException {
        //设置线程池中新建线程的名称
        BlockingQueue<Runnable> limitArray = new ArrayBlockingQueue<>(blockingQueueSize);
        ThreadFactory threadFactory = new ThreadFactory() {
            AtomicLong mAtomicLong = new AtomicLong(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "My-Thread-" + mAtomicLong.getAndIncrement());
                // thread.setDaemon(true);
                // 创建新线程的时候打印新线程名字
                logger.info("Create new Thread(): " + thread.getName());
                return thread;
            }
        };
        //自定义饱和策略：实现接口RejectedExecutionHandler
//        RejectedExecutionHandler handler = new RejectedExecutionHandler() {
//            @Override
//            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//                // 打印被拒绝的任务
//                logger.info("rejectedExecution:" + r.toString());
//            }
//        };
        //定义线程池属性
        ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSiz,
                30, TimeUnit.SECONDS, limitArray, threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        Path parquetFile = new Path(path);
        MessageType schema = getSchema(parquetFile);
        if (schema == null) {
            logger.error("ParseParquetUtil get the schema failed");
            return;
        }
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), parquetFile);
        ParquetReader<Group> reader = builder.build();
        SimpleGroup group = null;

        AtomicLong id = new AtomicLong(1);
        int index=0;//用于选择使用哪个客户端执行put操作
        TableSyncClient client=null;
        while ((group = (SimpleGroup) reader.read()) != null) {
            HashMap<String, Object> map = new HashMap<>();
            String dataType = "";
            String columnName = "";
            for (int i = 0; i < schema.getFieldCount(); i++) {
                dataType = getDataType(group.getType().getType(i).toString());
                columnName = group.getType().getFieldName(i);
                if (dataType.equals("int32")) {
                    map.put(columnName, group.getInteger(i, 0));
                } else if (dataType.equals("int64")) {
                    map.put(columnName, group.getLong(i, 0));
                } else if (dataType.equals("int96")) {
                    map.put(columnName, new String(group.getInt96(i, 0).getBytes()));
                } else if (dataType.equals("float")) {
                    map.put(columnName, group.getFloat(i, 0));
                } else if (dataType.equals("double")) {
                    map.put(columnName, group.getDouble(i, 0));
                } else if (dataType.equals("boolean")) {
                    map.put(columnName, String.valueOf(group.getBoolean(i, 0)));
                } else if (dataType.equals("binary")) {
                    map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
                }
            }
            if(index==sum){
                index=0;
            }
            client=tableSyncClient[index];
            index++;
            executor.submit(new PutTask(String.valueOf(id.getAndIncrement()),client , tableName, map));
        }
//        try {
//            executor.awaitTermination(20, TimeUnit.SECONDS);
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

            executor.shutdown();

    }


    /**
     * @param path
     * @return
     * @throws IOException
     * @description 读取parquet文件获取schema
     */
    public static MessageType getSchema(Path path) throws IOException {
        Configuration configuration = new Configuration();
//         windows 下测试入库impala需要这个配置
//        System.setProperty("hadoop.home.dir",
//                "E:\\mvtech\\software\\hadoop-common-2.2.0-bin-master");
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration,
                path, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        return schema;
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

    public static void main(String[] args) {
//        nsc.dropTable(tableName);
//        createSchemaTable();
        try {
            parseParquetWrite(path);
        } catch (IOException e) {
            logger.error("failed");
            e.printStackTrace();
        }
    }
}

