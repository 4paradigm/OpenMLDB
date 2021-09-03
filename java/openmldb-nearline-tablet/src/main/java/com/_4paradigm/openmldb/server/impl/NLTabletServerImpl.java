/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.server.impl;

import com._4paradigm.openmldb.conf.NLTabletConfig;
import com._4paradigm.openmldb.proto.NLTablet;
import com._4paradigm.openmldb.proto.Common.ColumnDesc;
import com._4paradigm.openmldb.proto.Type.DataType;
import com._4paradigm.openmldb.server.NLTabletServer;
import com._4paradigm.openmldb.common.zk.ZKClient;
import com._4paradigm.openmldb.common.zk.ZKConfig;
import com._4paradigm.openmldb.server.StatusCode;
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
        // TODO(denglong) use hive metastore and s3
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
            log.error("fail to connect zookeeper {}", NLTabletConfig.ZK_CLUSTER);
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
        try {
            createTable(request.getDbName(), request.getTableName(), request.getPartitionKey(),
                    request.getColumnDescList());
            builder.setCode(StatusCode.SUCCESS).setMsg("ok");
        } catch (Exception e) {
            builder.setCode(StatusCode.CREATE_TABLE_FAILED).setMsg(e.getMessage());
            log.warn("fail to create table {}. error msg: {}", request.getTableName(), e.getMessage());
        }
        NLTablet.CreateTableResponse response = builder.build();
        return response;
    }

    public void createTable(String dbName, String tableName, String partitionKey, List<ColumnDesc> schema) throws Exception {
        TableIdentifier table = TableIdentifier.of(dbName, tableName);
        if (catalog.tableExists(table)) {
            throw new Exception(String.format("table %s already exists in db %s", tableName, dbName));
        }
        Schema icebergSchema = convertSchema(schema);
        PartitionSpec spec = PartitionSpec.builderFor(icebergSchema)
                .day(partitionKey)  // use day partition default
                .build();
        catalog.createTable(table, icebergSchema, spec);
        log.info("create table {} success", tableName);
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
            throw new Exception("invalid type " + dataType.toString());
        }
    }
}
