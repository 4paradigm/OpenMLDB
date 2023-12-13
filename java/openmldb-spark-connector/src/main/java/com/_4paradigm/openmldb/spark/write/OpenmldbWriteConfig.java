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

package com._4paradigm.openmldb.spark.write;

import com._4paradigm.openmldb.sdk.SdkOption;

import java.io.Serializable;

// Must serializable
public class OpenmldbWriteConfig implements Serializable {
    public final String dbName, tableName, zkCluster, zkPath, writerType;
    public final boolean putIfAbsent;

    public OpenmldbWriteConfig(String dbName, String tableName, SdkOption option, String writerType, boolean putIfAbsent) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.zkCluster = option.getZkCluster();
        this.zkPath = option.getZkPath();
        this.writerType = writerType;
        this.putIfAbsent = putIfAbsent;
        // TODO(hw): other configs in SdkOption
    }
}
