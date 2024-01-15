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

package com._4paradigm.openmldb.spark;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com._4paradigm.openmldb.spark.read.OpenmldbReadConfig;
import com._4paradigm.openmldb.spark.read.OpenmldbScanBuilder;
import com._4paradigm.openmldb.spark.write.OpenmldbWriteBuilder;
import com._4paradigm.openmldb.spark.write.OpenmldbWriteConfig;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OpenmldbTable implements SupportsWrite, SupportsRead {
    private final String dbName;
    private final String tableName;
    private final SdkOption option;
    private final String writerType;
    private final int insertMemoryUsageLimit;
    private SqlExecutor executor = null;

    private Set<TableCapability> capabilities;

    public OpenmldbTable(String dbName, String tableName, SdkOption option, String writerType, int insertMemoryUsageLimit) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.option = option;
        this.writerType = writerType;
        this.insertMemoryUsageLimit = insertMemoryUsageLimit;
        try {
            this.executor = new SqlClusterExecutor(option);
            // no need to check table exists, schema() will check it later
        } catch (SqlException e) {
            e.printStackTrace();
        }
        // TODO: cache schema & delete executor?
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        OpenmldbWriteConfig config = new OpenmldbWriteConfig(dbName, tableName, option, writerType, insertMemoryUsageLimit);
        return new OpenmldbWriteBuilder(config, info);
    }

    @Override
    public String name() {
        // TODO(hw): db?
        return tableName;
    }

    public static DataType sdkTypeToSparkType(int sqlType) {
        switch (sqlType) {
            case Types.BOOLEAN:
                return DataTypes.BooleanType;
            case Types.SMALLINT:
                return DataTypes.ShortType;
            case Types.INTEGER:
                return DataTypes.IntegerType;
            case Types.BIGINT:
                return DataTypes.LongType;
            case Types.FLOAT:
                return DataTypes.FloatType;
            case Types.DOUBLE:
                return DataTypes.DoubleType;
            case Types.VARCHAR:
                return DataTypes.StringType;
            case Types.DATE:
                return DataTypes.DateType;
            case Types.TIMESTAMP:
                return DataTypes.TimestampType;
            default:
                throw new IllegalArgumentException("No support for sql type " + sqlType);
        }
    }

    @Override
    public StructType schema() {
        try {
            Schema schema = executor.getTableSchema(dbName, tableName);
            List<Column> schemaList = schema.getColumnList();
            StructField[] fields = new StructField[schemaList.size()];
            for (int i = 0; i < schemaList.size(); i++) {
                Column column = schemaList.get(i);
                fields[i] = new StructField(column.getColumnName(), sdkTypeToSparkType(column.getSqlType()),
                        !column.isNotNull(), Metadata.empty());
            }
            return new StructType(fields);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            capabilities.add(TableCapability.BATCH_WRITE);
            capabilities.add(TableCapability.BATCH_READ);
        }
        return capabilities;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap caseInsensitiveStringMap) {
        OpenmldbReadConfig config = new OpenmldbReadConfig(dbName, tableName, option);
        return new OpenmldbScanBuilder(config);
    }
}
