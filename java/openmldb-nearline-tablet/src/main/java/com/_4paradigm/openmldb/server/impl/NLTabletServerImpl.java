package com._4paradigm.openmldb.server.impl;

import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.proto.NLTablet;
import com._4paradigm.openmldb.proto.Common.ColumnDesc;
import com._4paradigm.openmldb.proto.Type.DataType;
import com._4paradigm.openmldb.server.NLTabletServer;
import com._4paradigm.openmldb.zk.ZKClient;
import com._4paradigm.openmldb.zk.ZKConfig;
import com.sun.javafx.binding.StringFormatter;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.PartitionSpec;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NLTabletServerImpl implements NLTabletServer {
    private ZKClient zkClient;
    private static final String NLTabletPrefix = "NLTABLET_";
    private HadoopCatalog catalog;

    public NLTabletServerImpl() throws Exception {
        Configuration conf = new Configuration();
        //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        catalog = new HadoopCatalog(conf, NLTabletConfig.HDFS_PATH);
        if (!connectZookeeper()) {
            throw new Exception("connect zk error");
        }
    }

    public boolean connectZookeeper() throws Exception {
        ZKConfig config = ZKConfig.builder()
                .cluster(NLTabletConfig.ZK_CLUSTER)
                .namespace(NLTabletConfig.ZK_ROOTPATH)
                .sessionTimeout(NLTabletConfig.ZK_SESSION_TIMEOUT)
                .build();
        zkClient = new ZKClient(config);
        if (!zkClient.connect()) {
            log.error("fail to connect zookeeper");
            return false;
        }
        String endpoint = NLTabletConfig.HOST + ":" + NLTabletConfig.PORT;
        String value = NLTabletPrefix + endpoint;
        zkClient.createEphemeralNode("nodes/" + value, value.getBytes());
        return true;
    }

    @Override
    public NLTablet.CreateTableResponse createTable(NLTablet.CreateTableRequest request) {
        NLTablet.CreateTableResponse.Builder builder = NLTablet.CreateTableResponse.newBuilder();
        String msg = new String();
        if (createTable(request.getDbName(), request.getTableName(), request.getPartitionKey(),
                request.getColumnDescList(), msg)) {
            builder.setCode(0).setMsg("ok");
        } else {
            builder.setCode(-1).setMsg(msg);
        }
        NLTablet.CreateTableResponse response = builder.build();
        return response;
    }

    public boolean createTable(String dbName, String tableName, String partitionKey, List<ColumnDesc> schema, String msg) {
        TableIdentifier table = TableIdentifier.of(dbName, tableName);
        if (catalog.tableExists(table)) {
            msg = String.format("table {} already exists in db {}", tableName, dbName);
            log.warn(msg);
            return false;
        }
        Schema icebergSchema = null;
        try {
            icebergSchema = convertSchema(schema);
        } catch (Exception e) {
            msg = String.format("fail to create table {}. convert schema error", tableName);
            log.warn(msg);
            return false;
        }
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .day(partitionKey)  // use day partition default
                .build();
        try {
            catalog.createTable(table, icebergSchema, spec);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        log.info("create table {} success", tableName);
        return true;
    }

    public Schema convertSchema(List<ColumnDesc> schema) throws Exception {
        List<Types.NestedField> columns = new ArrayList<Types.NestedField>();
        try {
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc col = schema.get(i);
            columns.add(Types.NestedField.required(i + 1, col.getName(), convertType(col.getDataType())));
        }
        } catch (Exception e) {
            throw e;
        }
        return new Schema(columns);
    }

    public Type.PrimitiveType convertType(DataType dataType) throws Exception {
        if (dataType == DataType.kBool) {
            return Types.BooleanType.get();
        } else if (dataType == DataType.kString || dataType == DataType.kVarchar) {
            return Types.StringType.get();
        } else if (dataType == DataType.kSmallInt) {
            return Types.IntegerType.get();
        } else if (dataType == DataType.kInt) {
            return Types.IntegerType.get();
        } else if (dataType == DataType.kBigInt) {
            return Types.LongType.get();
        } else if (dataType == DataType.kFloat) {
            return Types.FloatType.get();
        } else if (dataType == DataType.kDouble) {
            return Types.DoubleType.get();
        } else if (dataType == DataType.kDate) {
            return Types.DateType.get();
        } else if (dataType == DataType.kTimestamp) {
            return Types.TimestampType.withoutZone();
        } else {
            log.warn("invalid type {}", dataType.toString());
            throw new Exception("invalid type");
        }
    }
}
