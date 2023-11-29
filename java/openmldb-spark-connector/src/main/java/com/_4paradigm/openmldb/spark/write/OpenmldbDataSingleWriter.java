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

import com._4paradigm.openmldb.sdk.Schema;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public class OpenmldbDataSingleWriter implements DataWriter<InternalRow> {
    private final int partitionId;
    private final long taskId;
    private PreparedStatement preparedStatement = null;

    public OpenmldbDataSingleWriter(OpenmldbWriteConfig config, int partitionId, long taskId) {
        try {
            SdkOption option = new SdkOption();
            option.setZkCluster(config.zkCluster);
            option.setZkPath(config.zkPath);
            option.setIsLight(true);
            SqlClusterExecutor executor = new SqlClusterExecutor(option);
            String dbName = config.dbName;
            String tableName = config.tableName;

            Schema schema = executor.getTableSchema(dbName, tableName);
            // create insert placeholder
            StringBuilder insert = new StringBuilder("insert into " + tableName + " values(?");
            for (int i = 1; i < schema.getColumnList().size(); i++) {
                insert.append(",?");
            }
            insert.append(");");
            preparedStatement = executor.getInsertPreparedStmt(dbName, insert.toString());
        } catch (SQLException | SqlException e) {
            e.printStackTrace();
        }

        this.partitionId = partitionId;
        this.taskId = taskId;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        try {
            // record to openmldb row
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            Preconditions.checkState(record.numFields() == metaData.getColumnCount());
            OpenmldbDataWriter.addRow(record, preparedStatement);
            preparedStatement.execute();
        } catch (Exception e) {
            throw new IOException("write row to openmldb failed on " + record, e);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException("commit error", e);
        }
        // TODO(hw): need to return new WriterCommitMessageImpl(partitionId, taskId); ?
        return null;
    }

    @Override
    public void abort() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException("abort error", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException("close error", e);
        }
    }
}
