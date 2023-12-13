/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.spark;

import com._4paradigm.openmldb.sdk.SdkOption;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class OpenmldbSource implements TableProvider, DataSourceRegister {
    private final String DB = "db";
    private final String TABLE = "table";
    private final String ZK_CLUSTER = "zkCluster";
    private final String ZK_PATH = "zkPath";

    private String dbName;
    private String tableName;
    private SdkOption option = null;
    // single: insert when read one row
    // batch: insert when commit(after read a whole partition)
    private String writerType = "single";
    private boolean putIfAbsent = false;

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        Preconditions.checkNotNull(dbName = options.get(DB));
        Preconditions.checkNotNull(tableName = options.get(TABLE));

        String zkCluster = options.get(ZK_CLUSTER);
        String zkPath = options.get(ZK_PATH);
        Preconditions.checkNotNull(zkCluster);
        Preconditions.checkNotNull(zkPath);
        option = new SdkOption();
        option.setZkCluster(zkCluster);
        option.setZkPath(zkPath);
        option.setLight(true);
        String timeout = options.get("sessionTimeout");
        if (timeout != null) {
            option.setSessionTimeout(Integer.parseInt(timeout));
        }
        timeout = options.get("requestTimeout");
        if (timeout != null) {
            option.setRequestTimeout(Integer.parseInt(timeout));
        }
        String debug = options.get("debug");
        if (debug != null) {
            option.setEnableDebug(Boolean.valueOf(debug));
        }

        if (options.containsKey("writerType")) {
            writerType = options.get("writerType");
        }
        if (options.containsKey("putIfAbsent")) {
            putIfAbsent = Boolean.valueOf(options.get("putIfAbsent"));
        }

        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new OpenmldbTable(dbName, tableName, option, writerType, putIfAbsent);
    }

    @Override
    public String shortName() {
        return "openmldb";
    }
}
