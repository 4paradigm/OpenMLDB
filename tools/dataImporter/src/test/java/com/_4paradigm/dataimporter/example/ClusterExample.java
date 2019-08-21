package com._4paradigm.dataimporter.example;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.ns.NS;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ClusterExample {
    private static String zkEndpoints = "172.27.128.37:7181,172.27.128.37:7182,172.27.128.37:7183";  // 配置zk地址, 和集群启动配置中的zk_cluster保持一致
    private static String zkRootPath = "/rtidb_cluster";   // 配置集群的zk根路径, 和集群启动配置中的zk_root_path保持一致
    // 下面这几行变量定义不需要改
    private static String leaderPath = zkRootPath + "/leader";
    // NameServerClientImpl要么做成单例, 要么用完之后就调用close, 否则会导致fd泄露
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient clusterClient = null;
    // 发送同步请求的client
    private static TableSyncClient tableSyncClient = null;
    // 发送异步请求的client
    private static TableAsyncClient tableAsyncClient = null;

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
            tableSyncClient = new TableSyncClientImpl(clusterClient);
            tableAsyncClient = new TableAsyncClientImpl(clusterClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ClusterExample clusterExample = new ClusterExample();
//        clusterExample.createKVTable();//创建k-v表
        clusterExample.createSchemaTable();//创建schema表
//        clusterExample.syncKVTable();//同步操作k-v表
//        clusterExample.syncSchemaTable();//同步操作schenma表
//        clusterExample.AsyncKVTable();//异步操作k-v表
        clusterExample.AsyncSchemaTable();//异步操作schenma表
//        clusterExample.Traverse();//遍历schenma表
    }

    // 创建kv表
    public void createKVTable() {
        String name = "test1";
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder = NS.TableInfo.newBuilder()
                .setName(name)  // 设置表名
                //.setReplicaNum(3)    // 设置副本数, 此设置是可选的, 默认为3
                //.setPartitionNum(8)  // 设置分片数, 此设置是可选的, 默认为16
                //.setCompressType(NS.CompressType.kSnappy) // 设置数据压缩类型, 此设置是可选的默认为不压缩
                //.setTtlType("kLatestTime")  // 设置ttl类型, 此设置是可选的, 默认为"kAbsoluteTime"按时间过期
                .setTtl(144000);      // 设置ttl
        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        System.out.println("是否创建k-v表成功：" + ok);
        clusterClient.refreshRouteTable();
    }

    // 创建有schema的表
    public void createSchemaTable() {
        String name = "test2";
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder = NS.TableInfo.newBuilder()
                .setName(name)  // 设置表名
                //.setReplicaNum(3)    // 设置副本数. 此设置是可选的, 默认为3
                //.setPartitionNum(8)  // 设置分片数. 此设置是可选的, 默认为16
                //.setCompressType(NS.CompressType.kSnappy) // 设置数据压缩类型. 此设置是可选的默认为不压缩
                //.setTtlType("kLatestTime")  // 设置ttl类型. 此设置是可选的, 默认为"kAbsoluteTime"按时间过期
                .setTtl(0);      // 设置ttl. 如果ttl类型是kAbsoluteTime, 那么ttl的单位是分钟.
        // 设置schema信息
        NS.ColumnDesc col0 = NS.ColumnDesc.newBuilder()
                .setName("card")    // 设置字段名
                .setAddTsIdx(true)  // 设置是否为index, 如果设置为true表示该字段为维度列, 查询的时候可以通过此列来查询, 否则设置为false
                .setType("string")  // 设置字段类型, 支持的字段类型有[int32, uint32, int64, uint64, float, double, string]
                .build();
        NS.ColumnDesc col1 = NS.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        NS.ColumnDesc col2 = NS.ColumnDesc.newBuilder().setName("money").setAddTsIdx(false).setType("double").build();
        // 将schema添加到builder中
        builder.addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2);
        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        System.out.println("是否创建schema表成功：" + ok);
        clusterClient.refreshRouteTable();
    }

    // kv表put, scan, get
    public void syncKVTable() {
        String name = "test1";
        try {
            // 插入时指定的ts精确到毫秒
            long ts = System.currentTimeMillis();
            // 通过返回值可以判断是否插入成功
            boolean ret = tableSyncClient.put(name, "key1", ts, "value0");
            ret = tableSyncClient.put(name, "key1", ts + 1, "value1");
            ret = tableSyncClient.put(name, "key2", ts + 2, "value2");

            // scan数据, 查询范围需要传入st和et分别表示起始时间和结束时间, 其中起始时间大于结束时间
            // 如果结束时间et设置为0, 返回起始时间之前的所有数据
            KvIterator it = tableSyncClient.scan(name, "key1", ts + 1, 0);
            while (it.valid()) {
                byte[] buffer = new byte[it.getValue().remaining()];
                it.getValue().get(buffer);
                String value = new String(buffer);
                System.out.println("1:" + value);
                it.next();
            }

            // 可以通过limit限制返回条数
            // 如果st和et都设置为0则返回最近N条记录
            int limit = 1;
            it = tableSyncClient.scan(name, "key1", ts + 1, 0, limit);
            while (it.valid()) {
                byte[] buffer = new byte[it.getValue().remaining()];
                it.getValue().get(buffer);
                String value = new String(buffer);
                System.out.println("2:" + value);
                it.next();
            }

            // get数据, 查询指定ts的值. 如果ts设置为0, 返回最新插入的一条数据
            ByteString bs = tableSyncClient.get(name, "key1", ts);
            // 如果没有查询到bs就是null
            if (bs != null) {
                String value = new String(bs.toByteArray());
                System.out.println("3:" + value);
            }
            bs = tableSyncClient.get(name, "key1");
            // 如果没有查询到bs就是null
            if (bs != null) {
                String value = new String(bs.toByteArray());
                System.out.println("4:" + value);
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (TabletException e) {
            e.printStackTrace();
        }
    }

    // schema表put, scan, get
    public void syncSchemaTable() {
        String name = "test2";
        try {
            // 插入数据. 插入时指定的ts精确到毫秒
            long ts = System.currentTimeMillis();
            // 通过map传入需要插入的数据, key是字段名, value是该字段对应的值
            Map<String, Object> row = new HashMap<String, Object>();
            row.put("card", new String("card5"));
            row.put("mcc", "mcc0");
            row.put("money", 1.3d);
            // 通过返回值可以判断是否插入成功
            boolean ret = tableSyncClient.put(name, ts, row);
            row.clear();
            row.put("card", "card0");
            row.put("mcc", "mcc1");
            row.put("money", new Double("222"));
            ret = tableSyncClient.put(name, ts + 1, row);

            // 也可以通过object数组方式插入
            // 数组顺序和创建表时schema顺序对应
            // 通过RTIDBClusterClient可以获取表schema信息
             List<ColumnDesc> schema = clusterClient.getHandler(name).getSchema();
            Object[] arr = new Object[]{"card1", "mcc1", new Double("111")};
            ret = tableSyncClient.put(name, ts + 2, arr);

            // scan数据
            // key是需要查询字段的值, idxName是需要查询的字段名
            // 查询范围需要传入st和et分别表示起始时间和结束时间, 其中起始时间大于结束时间
            // 如果结束时间et设置为0, 返回起始时间之前的所有数据
            KvIterator it = tableSyncClient.scan(name, "card0", "card", ts + 1, 0);
            while (it.valid()) {
                // 解码出来是一个object数组, 顺序和创建表时的schema对应
                // 通过RTIDBClusterClient可以获取表schema信息
                // List<ColumnDesc> schema = clusterClient.getHandler(name).getSchema();
                Object[] result = it.getDecodedValue();
                System.out.println("1:" + result[0]);
                System.out.println("1:" + result[1]);
                System.out.println("1:" + result[2]);
                it.next();
            }
            // 可以通过limit限制返回条数
            // 如果st和et都设置为0则返回最近N条记录
            int limit = 1;
            it = tableSyncClient.scan(name, "card0", "card", ts + 1, 0, limit);
            while (it.valid()) {
                Object[] result = it.getDecodedValue();
                System.out.println("2:" + result[0]);
                System.out.println("2:" + result[1]);
                System.out.println("2:" + result[2]);
                it.next();
            }

            // get数据
            // key是需要查询字段的值, idxName是需要查询的字段名
            // 查询指定ts的值. 如果ts设置为0, 返回最新插入的一条数据

            // 解码出来是一个object数组, 顺序和创建表时的schema对应
            // 通过RTIDBClusterClient可以获取表schema信息
            // List<ColumnDesc> schema = clusterClient.getHandler(name).getSchema();
            Object[] result = tableSyncClient.getRow(name, "card0", "card", ts);
            for (int idx = 0; idx < result.length; idx++) {
                System.out.println("3:" + result[idx]);
            }
            result = tableSyncClient.getRow(name, "card0", "card", 0);
            for (int idx = 0; idx < result.length; idx++) {
                System.out.println("4:" + result[idx]);
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (TabletException e) {
            e.printStackTrace();
        }

    }

    // kv表异步put, 异步scan, 异步get
    public void AsyncKVTable() {
        String name = "test2";
        try {
            // 插入时指定的ts精确到毫秒
            long ts = System.currentTimeMillis();
            PutFuture pf1 = tableAsyncClient.put(name, "akey1", ts, "value4");
            PutFuture pf2 = tableAsyncClient.put(name, "akey1", ts + 1, "value5");
            PutFuture pf3 = tableAsyncClient.put(name, "akey2", ts + 2, "value6");
            // 调用get会阻塞到返回
            // 通过返回值可以判断是否插入成功
            boolean ret = pf1.get();
            ret = pf2.get();
            ret = pf3.get();

            // scan数据, 查询范围需要传入st和et分别表示起始时间和结束时间, 其中起始时间大于结束时间
            // 如果结束时间et设置为0, 返回起始时间之前的所有数据
            ScanFuture sf = tableAsyncClient.scan(name, "akey1", ts + 1, 0);
            KvIterator it = sf.get();
            while (it.valid()) {
                byte[] buffer = new byte[it.getValue().remaining()];
                it.getValue().get(buffer);
                String value = new String(buffer);
                System.out.println("1:" + value);
                it.next();
            }

            // 可以通过limit限制返回条数
            // 如果st和et都设置为0则返回最近N条记录
            int limit = 1;
            sf = tableAsyncClient.scan(name, "akey1", ts + 1, 0, limit);
            it = sf.get();
            while (it.valid()) {
                byte[] buffer = new byte[it.getValue().remaining()];
                it.getValue().get(buffer);
                String value = new String(buffer);
                System.out.println("2:" + value);
                it.next();
            }

            // get数据, 查询指定ts的值. 如果ts设置为0, 返回最新插入的一条数据
            GetFuture gf = tableAsyncClient.get(name, "akey1", ts);
            ByteString bs = gf.get();
            // 如果没有查询到bs就是null
            if (bs != null) {
                String value = new String(bs.toByteArray());
                System.out.println("3:" + value);
            }
            gf = tableAsyncClient.get(name, "akey1");
            bs = gf.get();
            // 如果没有查询到bs就是null
            if (bs != null) {
                String value = new String(bs.toByteArray());
                System.out.println("4:" + value);
            }
        } catch (TabletException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // schema表异步put, 异步scan, 异步get
    public void AsyncSchemaTable() {
        String name = "test2";
        try {
            // 插入数据. 插入时指定的ts精确到毫秒
            long ts = System.currentTimeMillis();
            // 通过map传入需要插入的数据, key是字段名, value是该字段对应的值
            Map<String, Object> row = new HashMap<String, Object>();
            row.put("card", "acard0");
            row.put("mcc", "amcc0");
            row.put("money", 1.3f);
            PutFuture pf1 = tableAsyncClient.put(name, ts, row);
            row.clear();
            row.put("card", "acard0");
            row.put("mcc", "amcc1");
            row.put("money", 15.8f);
            PutFuture pf2 = tableAsyncClient.put(name, ts + 1, row);

            // 也可以通过object数组方式插入
            // 数组顺序和创建表时schema顺序对应
            // 通过RTIDBClusterClient可以获取表schema信息
            // List<ColumnDesc> schema = clusterClient.getHandler(name).getSchema();
            Object[] arr = new Object[]{"acard1", "amcc1", 9.15f};
            PutFuture pf3 = tableAsyncClient.put(name, ts + 2, arr);
            // 调用get会阻塞到返回
            // 通过返回值可以判断是否插入成功
            boolean ret = pf1.get();
            ret = pf2.get();
            ret = pf3.get();

            // scan数据
            // key是需要查询字段的值, idxName是需要查询的字段名
            // 查询范围需要传入st和et分别表示起始时间和结束时间, 其中起始时间大于结束时间
            // 如果结束时间et设置为0, 返回起始时间之前的所有数据
            ScanFuture sf = tableAsyncClient.scan(name, "acard0", "card", ts + 1, 0);
            KvIterator it = sf.get();
            while (it.valid()) {
                // 解码出来是一个object数组, 顺序和创建表时的schema对应
                // 通过RTIDBClusterClient可以获取表schema信息
                // List<ColumnDesc> schema = clusterClient.getHandler(name).getSchema();
                Object[] result = it.getDecodedValue();
                System.out.println("1:" + result[0]);
                System.out.println("1:" + result[1]);
                System.out.println("1:" + result[2]);
                it.next();
            }
            // 可以通过limit限制返回条数
            // 如果st和et都设置为0则返回最近N条记录
            int limit = 1;
            sf = tableAsyncClient.scan(name, "acard0", "card", ts + 1, 0, limit);
            it = sf.get();
            while (it.valid()) {
                Object[] result = it.getDecodedValue();
                System.out.println("2:" + result[0]);
                System.out.println("2:" + result[1]);
                System.out.println("2:" + result[2]);
                it.next();
            }

            // get数据
            // key是需要查询字段的值, idxName是需要查询的字段名
            // 查询指定ts的值. 如果ts设置为0, 返回最新插入的一条数据

            // 解码出来是一个object数组, 顺序和创建表时的schema对应
            // 通过RTIDBClusterClient可以获取表schema信息
            // List<ColumnDesc> schema = clusterClient.getHandler(name).getSchema();
            GetFuture gf = tableAsyncClient.get(name, "acard0", "card", ts);
            Object[] result = gf.getRow();
            for (int idx = 0; idx < result.length; idx++) {
                System.out.println("3:" + result[idx]);
            }
            gf = tableAsyncClient.get(name, "acard0", "card", 0);
            result = gf.getRow();
            for (int idx = 0; idx < result.length; idx++) {
                System.out.println("4:" + result[idx]);
            }
        } catch (TabletException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // 遍历全表
    // 建议将removeDuplicateByTime设为true, 避免同一个key下相同ts比较多时引起client死循环
    // 建议将重试次数MaxRetryCnt往大设下, 默认值为1.
    public void Traverse() {
        String name = "test2";
        try {
            HashMap rowMap = null;
            for (int i = 0; i < 1000; i++) {
                rowMap = new HashMap<String, Object>();
                rowMap.put("card", "card" + (i + 9529));
                rowMap.put("mcc", "mcc" + i);
                rowMap.put("money", 9.2f);
                boolean ok = tableSyncClient.put(name, i + 9529, rowMap);
                System.out.println(ok+" "+i);
            }
            KvIterator it = tableSyncClient.traverse(name, "card");
            while (it.valid()) {
                // 一次迭代只能调用一次getDecodedValue
                Object[] row = it.getDecodedValue();
                System.out.println(row[0] + " " + row[1] + " " + row[2]);
                it.next();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
