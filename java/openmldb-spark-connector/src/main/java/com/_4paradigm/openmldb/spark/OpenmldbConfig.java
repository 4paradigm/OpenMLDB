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

import java.io.Serializable;

import org.sparkproject.guava.base.Preconditions;

// Must serializable
public class OpenmldbConfig implements Serializable {
    public final static String DB = "db";
    public final static String TABLE = "table";
    public final static String ZK_CLUSTER = "zkCluster";
    public final static String ZK_PATH = "zkPath";

    /* read&write */
    private String dbName;
    private String tableName;
    private SdkOption option = null;

    /* write */
    // single: insert when read one row
    // batch: insert when commit(after read a whole partition)
    private String writerType = "single";
    private int insertMemoryUsageLimit = 0;
    private boolean putIfAbsent = false;

    public OpenmldbConfig() {
    }

    public void setDB(String dbName) {
        Preconditions.checkArgument(dbName != null && !dbName.isEmpty(), "db name must not be empty");
        this.dbName = dbName;
    }

    public String getDB() {
        return this.dbName;
    }

    public void setTable(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "table name must not be empty");
        this.tableName = tableName;
    }

    public String getTable() {
        return this.tableName;
    }

    public void setSdkOption(SdkOption option) {
        this.option = option;
    }

    public SdkOption getSdkOption() {
        return this.option;
    }

    public void setWriterType(String string) {
        Preconditions.checkArgument(string.equals("single") || string.equals("batch"),
                "writerType must be 'single' or 'batch'");
        this.writerType = string;
    }

    public void setInsertMemoryUsageLimit(int int1) {
        Preconditions.checkArgument(int1 >= 0, "insert_memory_usage_limit must be >= 0");
        this.insertMemoryUsageLimit = int1;
    }

    public void setPutIfAbsent(Boolean valueOf) {
        this.putIfAbsent = valueOf;
    }

    public boolean isBatchWriter() {
        return this.writerType.equals("batch");
    }

    public boolean putIfAbsent() {
        return this.putIfAbsent;
    }

    public int getInsertMemoryUsageLimit() {
        return this.insertMemoryUsageLimit;
    }

}
