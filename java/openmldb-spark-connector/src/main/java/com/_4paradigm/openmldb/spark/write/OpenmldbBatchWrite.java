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

import com._4paradigm.openmldb.spark.OpenmldbConfig;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

public class OpenmldbBatchWrite implements BatchWrite {
    private final OpenmldbConfig config;
    private final LogicalWriteInfo info;

    public OpenmldbBatchWrite(OpenmldbConfig config, LogicalWriteInfo info) {
        this.config = config;
        this.info = info;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new OpenmldbDataWriterFactory(config);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

    }
}
