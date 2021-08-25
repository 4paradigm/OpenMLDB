package com._4paradigm.openmldb.server.impl;

import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.server.NLTablet;
import com._4paradigm.openmldb.common.Common.ColumnDesc;
import com._4paradigm.openmldb.type.Type.DataType;
import com._4paradigm.openmldb.server.NLTabletServer;
import com._4paradigm.openmldb.zk.ZKClient;
import com._4paradigm.openmldb.zk.ZKConfig;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Table;
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

    public NLTabletServerImpl() throws Exception {
        try {
            connectZookeeper();
        } catch (Exception e) {
            log.error("init zk error");
            throw e;
        }
    }

    public void connectZookeeper() throws Exception {
        ZKConfig config = ZKConfig.builder()
                .cluster(NLTabletConfig.ZK_CLUSTER)
                .namespace(NLTabletConfig.ZK_ROOTPATH)
                .sessionTimeout(NLTabletConfig.ZK_SESSION_TIMEOUT)
                .build();
        zkClient = new ZKClient(config);
        zkClient.connect();
        String endpoint = NLTabletConfig.HOST + ":" + NLTabletConfig.PORT;
        String value = NLTabletPrefix + endpoint;
        zkClient.createEphemeralNode("nodes/" + value, value.getBytes());
    }

    @Override
    public NLTablet.CreateTableResponse createTable(NLTablet.CreateTableRequest request) {
        NLTablet.CreateTableResponse.Builder builder = NLTablet.CreateTableResponse.newBuilder();
        if (CreateTable(request.getDbName(), request.getTableName(), request.getPartitionKey(), request.getColumnDescList())) {
            builder.setCode(0).setMsg("ok");
        } else {
            builder.setCode(-1).setMsg("create table failed");
        }
        NLTablet.CreateTableResponse response = builder.build();
        return response;
    }

    public boolean CreateTable(String dbName, String tableName, String partitionKey, List<ColumnDesc> schema) {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        HadoopCatalog catalog = new HadoopCatalog(conf, NLTabletConfig.HDFS_PATH);
        TableIdentifier name = TableIdentifier.of(dbName, tableName);
        Schema icebergSchema = null;
        try {
            icebergSchema = ConvertSchema(schema);
        } catch (Exception e) {
            log.error("fail to create table {}", name);
            return false;
        }
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .day(partitionKey)  // use day partition default
                .build();
        catalog.createTable(name, icebergSchema, spec);
        log.info("create table {} success", name);
        return true;
    }

    public Schema ConvertSchema(List<ColumnDesc> schema) throws Exception {
        List<Types.NestedField> columns = new ArrayList<Types.NestedField>();
        try {
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc col = schema.get(i);
            columns.add(Types.NestedField.required(i + 1, col.getName(), ConvertType(col.getDataType())));
        }
        } catch (Exception e) {
            throw e;
        }
        return new Schema(columns);
    }

    public Type.PrimitiveType ConvertType(DataType dataType) throws Exception {
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
            log.error("invalid type {}", dataType.toString());
            throw new Exception("invalid type");
        }
    }
}
