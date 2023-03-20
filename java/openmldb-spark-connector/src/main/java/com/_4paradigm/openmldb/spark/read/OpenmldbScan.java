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

package com._4paradigm.openmldb.spark.read;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;

public class OpenmldbScan implements Scan, Batch {
    private final OpenmldbReadConfig config;

    public OpenmldbScan(OpenmldbReadConfig config) {
        this.config = config;
    }

    @Override
    public StructType readSchema() {
        // TODO: No need to set and it is set in OpenmldbTable
        return null;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        // TODO: Need to pass the partition info
        return new InputPartition[]{new SimplePartition()};
    }

    @Override
    public Batch toBatch() {
        return this;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new OpenmldbPartitionReaderFactory(config);
    }
}
