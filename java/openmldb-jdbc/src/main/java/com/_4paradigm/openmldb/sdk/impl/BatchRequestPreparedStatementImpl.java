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

import com._4paradigm.openmldb.ColumnIndicesSet;
import com._4paradigm.openmldb.SQLRequestRowBatch;
import com._4paradigm.openmldb.SQLRouter;
import com._4paradigm.openmldb.Status;
import com._4paradigm.openmldb.jdbc.SQLResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;


public class BatchRequestPreparedStatementImpl extends RequestPreparedStatementImpl {
    private static final Logger logger = LoggerFactory.getLogger(BatchRequestPreparedStatementImpl.class);

    private ColumnIndicesSet commonColumnIndices;
    private SQLRequestRowBatch currentRowBatch;

    public BatchRequestPreparedStatementImpl(String db, String sql,
                                             SQLRouter router,
                                             List<Integer> commonColumnIdxList) throws SQLException {
        super(db, sql, router);
        this.commonColumnIndices = new ColumnIndicesSet(currentSchema);
        for (Integer idx : commonColumnIdxList) {
            if (idx != null) {
                this.commonColumnIndices.AddCommonColumnIdx(idx);
            }
        }
        this.currentRowBatch = new SQLRequestRowBatch(currentSchema, commonColumnIndices);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        checkExecutorClosed();
        Status status = new Status();
        com._4paradigm.openmldb.ResultSet resultSet = router.ExecuteSQLBatchRequest(
                db, currentSql, currentRowBatch, status);
        if (resultSet == null || status.getCode() != 0) {
            String msg = status.ToString();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            throw new SQLException("execute sql fail: " + msg);
        }
        status.delete();
        ResultSet rs = new SQLResultSet(resultSet);
        if (closeOnComplete) {
            closed = true;
        }
        return rs;
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
        if (this.currentRow == null || status.getCode() != 0) {
            String msg = status.ToString();
            status.delete();
            logger.error("getRequestRow failed: {}", msg);
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
