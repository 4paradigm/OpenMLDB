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
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class OpenmldbSource implements TableProvider, DataSourceRegister {

    private OpenmldbConfig config = new OpenmldbConfig();

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        config.setDB(options.get(OpenmldbConfig.DB));
        config.setTable(options.get(OpenmldbConfig.TABLE));

        SdkOption option = new SdkOption();
        option.setZkCluster(options.get(OpenmldbConfig.ZK_CLUSTER));
        option.setZkPath(options.get(OpenmldbConfig.ZK_PATH));
        option.setLight(true);
        config.setSdkOption(option);

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
            config.setWriterType(options.get("writerType"));
        }
        if (options.containsKey("putIfAbsent")) {
            config.setPutIfAbsent(Boolean.valueOf(options.get("putIfAbsent")));
        }

        if (options.containsKey("insert_memory_usage_limit")) {
            config.setInsertMemoryUsageLimit(Integer.parseInt(options.get("insert_memory_usage_limit")));
        }
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new OpenmldbTable(config);
    }

    @Override
    public String shortName() {
        return "openmldb";
    }
}
