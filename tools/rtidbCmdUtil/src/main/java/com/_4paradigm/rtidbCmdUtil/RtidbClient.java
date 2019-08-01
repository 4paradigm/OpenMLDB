package com._4paradigm.rtidbCmdUtil;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.ns.NS;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rtidb.client.Client;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RtidbClient {
    private static Logger logger = LoggerFactory.getLogger(RtidbClient.class);
    // NameServerClientImpl要么做成单例, 要么用完之后就调用close, 否则会导致fd泄露
    private static NameServerClientImpl nsc;
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient clusterClient = null;
    // 发送异步请求的client
    private static TableAsyncClient tableAsyncClient = null;
    // 发送同步请求的client
    private static TableSyncClient tableSyncClient = null;

    public RtidbClient(String zkEndpoints, String zkRootPath) {
        try {
            nsc = new NameServerClientImpl(zkEndpoints, zkRootPath + "/leader");
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
            tableAsyncClient = new TableAsyncClientImpl(clusterClient);
            tableSyncClient = new TableSyncClientImpl(clusterClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public NS.TableInfo getTableInfo(String fileName) {
        File file = new File(fileName);
        FileReader fileReader = null;
        NS.TableInfo tableInfo = null;
        Client.TableInfo.Builder cBuilder = Client.TableInfo.newBuilder();
        try {
            fileReader = new FileReader(file);
            TextFormat.merge(fileReader, cBuilder);
            Client.TableInfo cTableInfo = cBuilder.build();
            NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                    .setName(cBuilder.getName())
                    .setPartitionNum(cBuilder.getPartitionNum())
                    .setReplicaNum(cBuilder.getReplicaNum())
                    .setTtl(cBuilder.getTtl())
                    .setTtlType(cBuilder.getTtlType())
                    .setCompressType(NS.CompressType.valueOf(cBuilder.getCompressType()))
                    .setKeyEntryMaxHeight(cBuilder.getKeyEntryMaxHeight())
                    .addAllColumnDescV1(cBuilder.getColumnDescList())
                    .addAllColumnKey(cBuilder.getColumnKeyList());
            tableInfo = builder.build();
            return tableInfo;
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("file " + file.getAbsolutePath() + " did not exist!");
            System.exit(0);
        } finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return tableInfo;
    }

    //get schema
    public List<ColumnDesc> getSchema(String tableName) {
        List<ColumnDesc> columnDescList = new ArrayList<>();
        if (tableName == null || tableName.isEmpty()) {
            System.out.println("filePath is null or empty");
            return columnDescList;
        }
        try {
            columnDescList = clusterClient.getHandler(tableName).getTableInfo().getColumnDescV1List();
        } catch (Exception e) {
            System.out.println("table " + tableName + " did not exist");
            return null;
        }
        return columnDescList;
    }

    public List<com._4paradigm.rtidb.common.Common.ColumnKey> getColumnkey(String tableName) {
        List<com._4paradigm.rtidb.common.Common.ColumnKey> columnKeyList = new ArrayList<>();
        if (tableName == null || tableName.isEmpty()) {
            System.out.println("filePath is null or empty");
            return columnKeyList;
        }
        TableHandler handler = clusterClient.getHandler(tableName);
        try {
            columnKeyList = handler.getTableInfo().getColumnKeyList();
        } catch (Exception e) {
            System.out.println("table " + tableName + " did not exist");
            return columnKeyList;
        }
        return columnKeyList;
    }

    // create table_name ttl partition_num replica_num
    public boolean createKVTable(String tableName, String ttl, String partition_num, String replica_num, String... args) {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder.setName(tableName); //设置表名

        long time_to_live;
        try {
            time_to_live = Long.parseLong(ttl);
        } catch (NumberFormatException e) {
            if (ttl.startsWith("latest")) {
                builder.setTtlType("kLatestTime");
                try {
                    time_to_live = Long.parseLong(ttl.split(":")[1]);
                } catch (NumberFormatException e2) {
                    System.out.println("ttl should be uint64");
                    return false;
                }
            } else {
                System.out.println("ttl should be uint64");
                return false;
            }
        }
        if (time_to_live < 0) {
            System.out.println("ttl should be uint64");
            return false;
        }
        builder.setTtl(time_to_live); //设置ttl

        int pNum;
        try {
            pNum = Integer.parseInt(partition_num);
        } catch (NumberFormatException e) {
            System.out.println("partition_num should be uint32");
            return false;
        }
        if (pNum <= 0) {
            System.out.println("partition_num should be uint32");
            return false;
        }
        builder.setPartitionNum(pNum);  // 设置分片数, 此设置是可选的, 默认为8

        int rNum;
        try {
            rNum = Integer.parseInt(replica_num);
        } catch (NumberFormatException e) {
            System.out.println("replica_num should be uint32");
            return false;
        }
        if (rNum <= 0) {
            System.out.println("replica_num should be uint32");
            return false;
        }
        builder.setReplicaNum(rNum);    // 设置副本数, 此设置是可选的, 默认为3

        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        if (ok) {
            System.out.println("create table succeeded");
        } else {
            System.out.println("create table failed");
        }
        clusterClient.refreshRouteTable();
        return ok;
    }

    //create table_name ttl partition_num replica_num [colum_name1:type:index colum_name2:type ...]
    public boolean createSchemaTable(String... args) {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder.setName(args[1]); //设置表名

        long time_to_live;
        try {
            time_to_live = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            if (args[2].startsWith("latest")) {
                builder.setTtlType("kLatestTime");
                try {
                    time_to_live = Long.parseLong(args[2].split(":")[1]);
                } catch (NumberFormatException e2) {
                    System.out.println("ttl should be uint64");
                    return false;
                }
            } else {
                System.out.println("ttl should be uint64");
                return false;
            }
        }
        if (time_to_live < 0) {
            System.out.println("ttl should be uint64");
            return false;
        }
        builder.setTtl(time_to_live); //设置ttl

        int pNum;
        try {
            pNum = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.out.println("partition_num should be uint32");
            return false;
        }
        if (pNum <= 0) {
            System.out.println("partition_num should be uint32");
            return false;
        }
        builder.setPartitionNum(pNum);  // 设置分片数, 此设置是可选的, 默认为8

        int rNum;
        try {
            rNum = Integer.parseInt(args[4]);
        } catch (NumberFormatException e) {
            System.out.println("replica_num should be uint32");
            return false;
        }
        if (rNum <= 0) {
            System.out.println("replica_num should be uint32");
            return false;
        }
        builder.setReplicaNum(rNum);    // 设置副本数, 此设置是可选的, 默认为3

        if (args.length > 5) {
            for (int i = 5; i < args.length; i++) {
                ColumnDesc columnDesc = null;
                if (args[i].split(":").length == 2) {
                    columnDesc = ColumnDesc.newBuilder()
                            .setName(args[i].split(":")[0].trim())
                            .setType(args[i].split(":")[1].trim())
                            .build();
                } else if (args[i].split(":").length == 3) {
                    columnDesc = ColumnDesc.newBuilder()
                            .setName(args[i].split(":")[0].trim())
                            .setType(args[i].split(":")[1].trim())
                            .setAddTsIdx(true)
                            .build();
                }
                if (columnDesc == null) {
                    System.out.println("create failed! schema format is illegal");
                    return false;
                }
                builder.addColumnDescV1(columnDesc);
            }
        }

        NS.TableInfo table = builder.build();
        // 可以通过返回值判断是否创建成功
        boolean ok = nsc.createTable(table);
        if (ok) {
            System.out.println("create table succeeded");
        } else {
            System.out.println("create table failed");
        }
        clusterClient.refreshRouteTable();
        return ok;
    }

    public boolean createSchemaTableFromFile(String fileName) {
        boolean ok;
        try {
            NS.TableInfo tableInfo = getTableInfo(fileName);
            ok = nsc.createTable(tableInfo);
        } catch (Exception e) {
            System.out.println("createSchemaTableFromFile erro: " + e.toString());
            return false;
        }
        if (ok) {
            System.out.println("create table succeeded");
        } else {
            System.out.println("create table failed");
        }
        return ok;
    }

    //put table_name pk ts value
    public boolean putKv(String tableName, String key, String ts, String value) {
        long timestamp;
        try {
            timestamp = Long.parseLong(ts);
        } catch (Exception e) {
            System.out.println("timestamp should be uint64");
            return false;
        }
        if (timestamp <= 0) {
            System.out.println("timestamp should be uint64");
            return false;
        }
        boolean result = false;
        try {
            result = tableAsyncClient.put(tableName, key, timestamp, value).get();
        } catch (TabletException e) {
            System.out.println("putKv.erro1: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.out.println("putKv.erro2: " + e.getMessage());
            return false;
        }
        if (result) {
            System.out.println("Put ok");
        } else {
            System.out.println("Put failed");
        }
        return result;
    }

    //1、 put table_name ts value1 value2 value3 ...   2、put table_name value1 value2 value3 ...
    public boolean putSchema(String... args) {
        long timestamp = 0;
        int index = 2;
        if (clusterClient.getHandler(args[1]) != null && !clusterClient.getHandler(args[1]).hasTsCol()) {
            index = 3;
            try {
                timestamp = Long.parseLong(args[2]);
            } catch (Exception e) {
                System.out.println("timestamp should be uint64");
                return false;
            }
            if (timestamp <= 0) {
                System.out.println("timestamp should be uint64");
                return false;
            }
        }
        Map<String, Object> row = new HashMap<String, Object>();
        List<ColumnDesc> schema = getSchema(args[1]);
        boolean result;
        try {
            for (int i = 0; i < schema.size(); i++) {
                String type = schema.get(i).getType();
                switch (type) {
                    case "string":
                        row.put(schema.get(i).getName(), args[index + i]);
                        break;
                    case "int16":
                        row.put(schema.get(i).getName(), Short.valueOf(args[index + i]));
                        break;
                    case "int32":
                        row.put(schema.get(i).getName(), Integer.valueOf(args[index + i]));
                        break;
                    case "int64":
                        row.put(schema.get(i).getName(), Long.valueOf(args[index + i]));
                        break;
                    case "uint16":
                        row.put(schema.get(i).getName(), Short.valueOf(args[index + i]));
                        break;
                    case "uint32":
                        row.put(schema.get(i).getName(), Integer.valueOf(args[index + i]));
                        break;
                    case "uint64":
                        row.put(schema.get(i).getName(), Long.valueOf(args[index + i]));
                        break;
                    case "float":
                        row.put(schema.get(i).getName(), Float.valueOf(args[index + i]));
                        break;
                    case "double":
                        row.put(schema.get(i).getName(), Double.valueOf(args[index + i]));
                        break;
                    case "timestamp":
                        row.put(schema.get(i).getName(), Long.valueOf(args[index + i]));
                        break;
                    case "date":
                        row.put(schema.get(i).getName(), Date.valueOf(args[index + i]));
                        break;
                    case "bool":
                        row.put(schema.get(i).getName(), Boolean.valueOf(args[index + i]));
                        break;
                    default:
                }
            }
            if (!clusterClient.getHandler(args[1]).hasTsCol()) {
                result = tableAsyncClient.put(args[1], timestamp, row).get();
            } else {
                result = tableAsyncClient.put(args[1], row).get();
            }
        } catch (TabletException e) {
            System.out.println("putSchema.erro1: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.out.println("putSchema.erro2: " + e.getMessage());
            return false;
        }

        if (result) {
            System.out.println("Put ok");
        } else {
            System.out.println("Put failed");
        }
        return result;
    }

    //get table_name key ts
    public void getKvData(String tableName, String key, String ts) {
        long timestamp;
        try {
            timestamp = Long.parseLong(ts);
        } catch (Exception e) {
            System.out.println("timestamp should be uint64");
            return;
        }
        if (timestamp < 0) {
            System.out.println("timestamp should be more than 0");
            return;
        }
        String result = "";
        try {
            ByteString bytes = tableAsyncClient.get(tableName, key, timestamp).get();
            result = bytes.toStringUtf8();
        } catch (Exception e) {
            System.out.println("erro: " + e.getMessage());
            return;
        }
        System.out.println("value is: " + result);
    }

    //get table_name key col_name ts
    //get table_name key1|key2... idxname time tsName  eg. get test4 card1|mcc1 card_mcc 15608559166000 ts1
    public void getSchemaData(String... args) {
        long timestamp;
        try {
            timestamp = Long.parseLong(args[4]);
        } catch (Exception e) {
            System.out.println("timestamp should be uint64");
            return;
        }
        if (timestamp < 0) {
            System.out.println("timestamp should be more than 0");
            return;
        }
        TableHandler handler = clusterClient.getHandler(args[1]);
        List<ColumnDesc> columnDescV1List = handler.getTableInfo().getColumnDescV1List();
        int count = 1;
        System.out.print("#\t");
        for (ColumnDesc columnDesc : columnDescV1List) {
            System.out.print(columnDesc.getName() + "\t");
        }
        System.out.println();
        System.out.println("--------------------------");
        Object[] rows = null;
        try {
            if (!handler.hasTsCol()) {
                rows = tableSyncClient.getRow(args[1], args[2], args[3], timestamp);
            } else {
                rows = tableSyncClient.getRow(args[1], args[2], args[3], timestamp, args[5], null);
            }

        } catch (Exception e) {
            System.out.println("getSchemaData_erro: " + e.toString());
            return;
        }
        System.out.print(count + "\t");
        for (Object row : rows) {
            System.out.print(row + "\t");
            count++;
        }
        System.out.println();
    }

    public void previewKv(String tableName) {
        try {
            int count = 1;
            int limit = 100;
            KvIterator it = tableSyncClient.traverse(tableName);
            System.out.println("#\t" + "key\t" + "ts" + "\tdata");
            System.out.println("--------------------------");
            while (limit > 0 && it.valid()) {
                System.out.println(count + "\t" + it.getPK() + "\t" + it.getKey() + "\t" + Common.parseFromByteBuffer(it.getValue()));
                it.next();
                limit--;
                count++;
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (TabletException e) {
            e.printStackTrace();
        }
    }

    //preview tableName
    public void previewSchema(String tableName) {
        TableHandler handler = clusterClient.getHandler(tableName);
        List<ColumnDesc> columnDescV1List = handler.getTableInfo().getColumnDescV1List();
        List<com._4paradigm.rtidb.common.Common.ColumnKey> columnKeyList = handler.getTableInfo().getColumnKeyList();
        int count = 1;
        int limit = 100;
        System.out.print("#\t");
        for (ColumnDesc columnDesc : columnDescV1List) {
            System.out.print(columnDesc.getName() + "\t");
        }
        System.out.println();
        System.out.println("--------------------------");
        if ( handler.hasTsCol()) {
            try {
                String indexName = columnKeyList.get(0).getIndexName();
                String tsName = columnKeyList.get(0).getTsName(0);
                KvIterator it = tableSyncClient.traverse(tableName, indexName, tsName);
                while (limit > 0 && it.valid()) {
                    System.out.print(count + "\t");
                    for (Object row : it.getDecodedValue()) {
                        System.out.print(row + "\t");
                    }
                    System.out.println();
                    it.next();
                    limit--;
                    count++;
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (TabletException e) {
                e.printStackTrace();
            }
        } else{
            try {
                String indexName = columnKeyList.get(0).getIndexName();
                KvIterator it = tableSyncClient.traverse(tableName, indexName);
                while (limit > 0 && it.valid()) {
                    System.out.print(count + "\t");
                    for (Object row : it.getDecodedValue()) {
                        System.out.print(row + "\t");
                    }
                    System.out.println();
                    it.next();
                    limit--;
                    count++;
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (TabletException e) {
                e.printStackTrace();
            }
        }
    }

    //scan table_name pk start_time end_time [limit]
    public void scanKv(String... args) {
        long start_time;
        try {
            start_time = Long.parseLong(args[3]);
        } catch (Exception e) {
            System.out.println("start_time should be uint64");
            return;
        }
        if (start_time <= 0) {
            System.out.println("start_time should be uint64");
            return;
        }

        long end_time;
        try {
            end_time = Long.parseLong(args[4]);
        } catch (Exception e) {
            System.out.println("end_time should be uint64");
            return;
        }
        if (end_time < 0) {
            System.out.println("end_time should more than zero");
            return;
        }
        int limit = 0;
        if (args.length == 6) {
            try {
                limit = Integer.parseInt(args[5]);
            } catch (Exception e) {
                System.out.println("limit should be uint32");
                return;
            }
            if (limit <= 0) {
                System.out.println("limit should be uint32");
                return;
            }
        }
        try {
            KvIterator it = null;
            if (args.length == 5) {
                it = tableSyncClient.scan(args[1], args[2], start_time, end_time);
            } else if (args.length == 6) {
                it = tableSyncClient.scan(args[1], args[2], start_time, end_time, limit);
            } else {
                System.out.println("scan format error. eg: scan table_name pk start_time end_time [limit] | scan table_name key key_name start_time end_time [limit] | scan table_name key1|key2.. col_name start_time end_time tsName [limit]");
                return;
            }
            int count = 1;
            System.out.println("#\t" + "key\t" + "ts" + "\tdata");
            System.out.println("--------------------------");
            while (it != null && it.valid()) {
                System.out.println(count + "\t" + args[2] + "\t" + it.getKey() + "\t" + Common.parseFromByteBuffer(it.getValue()));
                it.next();
                count++;
            }
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (TabletException e) {
            e.printStackTrace();
        }
    }

    //scan table_name key col_name start_time end_time [limit] eg.scan t1 card0 card 1535371622000 0
    //scan table_name key1|key2.. col_name start_time end_time tsName [limit]  eg. scan test4 card1|mcc0 card_mcc 15608559166000 0 ts1
    public void scanSchema(String... args) {
        TableHandler handler = clusterClient.getHandler(args[1]);
        List<ColumnDesc> columnDescV1List = handler.getTableInfo().getColumnDescV1List();
        int count = 1;
        System.out.print("#\t");
        for (ColumnDesc columnDesc : columnDescV1List) {
            System.out.print(columnDesc.getName() + "\t");
        }
        System.out.println();
        System.out.println("--------------------------");
        KvIterator it = null;
        long start_time;
        try {
            start_time = Long.parseLong(args[4]);
        } catch (Exception e) {
            System.out.println("start_time should be uint64");
            return;
        }
        if (start_time <= 0) {
            System.out.println("start_time should be uint64");
            return;
        }

        long end_time;
        try {
            end_time = Long.parseLong(args[5]);
        } catch (Exception e) {
            System.out.println("end_time should be uint64");
            return;
        }
        if (end_time < 0) {
            System.out.println("end_time should more than zero");
            return;
        }
        if (handler.hasTsCol()) {
            int limit = 0;
            if (args.length == 8) {
                try {
                    limit = Integer.parseInt(args[7]);
                } catch (Exception e) {
                    System.out.println("limit should be uint32");
                    return;
                }
                if (limit <= 0) {
                    System.out.println("limit should be uint32");
                    return;
                }
            }
            try {
                if (args.length > 6) {
                    it = tableSyncClient.scan(args[1], args[2], args[3], start_time, end_time, args[6], limit);
                } else {
                    System.out.println("scan format error. eg: scan table_name pk start_time end_time [limit] | scan table_name key key_name start_time end_time [limit] | scan table_name key1|key2.. col_name start_time end_time tsName [limit]");
                    return;
                }
                while (it != null && it.valid()) {
                    System.out.print(count + "\t");
                    for (Object row : it.getDecodedValue()) {
                        System.out.print(row + "\t");
                    }
                    System.out.println();
                    it.next();
                    count++;
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (TabletException e) {
                e.printStackTrace();
            }
        } else {
            int limit = 0;
            if (args.length == 7) {
                try {
                    limit = Integer.parseInt(args[6]);
                } catch (Exception e) {
                    System.out.println("limit should be uint32");
                    return;
                }
                if (limit <= 0) {
                    System.out.println("limit should be uint32");
                    return;
                }
            }
            try {
                if (args.length > 5) {
                    it = tableSyncClient.scan(args[1], args[2], args[3], start_time, end_time, limit);
                } else {
                    System.out.println("scan format error. eg: scan table_name pk start_time end_time [limit] | scan table_name key key_name start_time end_time [limit] | scan table_name key1|key2.. col_name start_time end_time tsName [limit]");
                    return;
                }
                while (it != null && it.valid()) {
                    System.out.print(count + "\t");
                    for (Object row : it.getDecodedValue()) {
                        System.out.print(row + "\t");
                    }
                    System.out.println();
                    it.next();
                    count++;
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (TabletException e) {
                e.printStackTrace();
            }
        }
    }
}
