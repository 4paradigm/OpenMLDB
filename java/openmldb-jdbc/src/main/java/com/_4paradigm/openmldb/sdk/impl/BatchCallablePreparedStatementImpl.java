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

package com._4paradigm.openmldb.sdk.impl;

import com._4paradigm.openmldb.*;

import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import com._4paradigm.openmldb.sdk.QueryFuture;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class BatchCallablePreparedStatementImpl extends CallablePreparedStatement {
    private ColumnIndicesSet commonColumnIndices;
    private SQLRequestRowBatch currentRowBatch;

    public BatchCallablePreparedStatementImpl(String db, String spName, SQLRouter router) throws SQLException {
        super(db, spName, router);
        this.commonColumnIndices = new ColumnIndicesSet(this.currentSchema);
        for (int i = 0; i < this.currentSchema.GetColumnCnt(); i++) {
            if (this.currentSchema.IsConstant(i)) {
                this.commonColumnIndices.AddCommonColumnIdx(i);
            }
        }
        this.currentRowBatch = new SQLRequestRowBatch(this.currentSchema, this.commonColumnIndices);
    }

    @Override
    public SQLResultSet executeQuery() throws SQLException {
        checkClosed();
        checkExecutorClosed();
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = router.ExecuteSQLBatchRequest(
                db, currentSql, currentRowBatch, status);
        if (status.getCode() != 0 || resultSet == null) {
            String msg = status.ToString();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            throw new SQLException("execute sql fail: " + msg);
        }
        status.delete();
        SQLResultSet rs = new SQLResultSet(resultSet);
        if (closeOnComplete) {
            closed = true;
        }
        return rs;
    }

    @Override
    public QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        checkClosed();
        checkExecutorClosed();
        Status status = new Status();
        com._4paradigm.openmldb.QueryFuture queryFuture = router.CallSQLBatchRequestProcedure(db, spName, unit.toMillis(timeOut), currentRowBatch, status);
        if (status.getCode() != 0 || queryFuture == null) {
            String msg = status.ToString();
            status.delete();
            if (queryFuture != null) {
                queryFuture.delete();
            }
            throw new SQLException("call procedure fail, msg: " + msg);
        }
        status.delete();
        return new QueryFuture(queryFuture);
    }

    @Override
    public void addBatch() throws SQLException {
        dataBuild();
        if (!this.currentRow.OK()) {
            throw new RuntimeException("not ok row");
        }
        currentRowBatch.AddRow(this.currentRow);
        this.currentRow.delete();
        Status status = new Status();
        this.currentRow = router.GetRequestRow(db, currentSql, status);
        if (status.getCode() != 0 || this.currentRow == null) {
            String msg = status.ToString();
            status.delete();
            throw new SQLException("getRequestRow failed!, msg: " + msg);
        }
        status.delete();
    }

    @Override
    public void clearBatch() throws SQLException {
        currentRowBatch.Clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException("Should use executeQuery() to get batch result");
    }

    @Override
    public void close() throws SQLException {
        super.close();
        if (commonColumnIndices != null) {
            commonColumnIndices.delete();
            commonColumnIndices = null;
        }
        if (currentRowBatch != null) {
            currentRowBatch.delete();
            currentRowBatch = null;
        }
    }
}
