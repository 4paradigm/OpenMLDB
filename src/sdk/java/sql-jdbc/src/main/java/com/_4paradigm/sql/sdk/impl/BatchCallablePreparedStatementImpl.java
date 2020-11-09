package com._4paradigm.sql.sdk.impl;

import com._4paradigm.sql.ColumnIndicesSet;
import com._4paradigm.sql.SQLRequestRowBatch;
import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.Status;
import com._4paradigm.sql.jdbc.CallablePreparedStatement;
import com._4paradigm.sql.jdbc.SQLResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BatchCallablePreparedStatementImpl extends CallablePreparedStatement {
    private static final Logger logger = LoggerFactory.getLogger(BatchCallablePreparedStatementImpl.class);
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
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        Status status = new Status();
        com._4paradigm.sql.ResultSet resultSet = router.ExecuteSQLBatchRequest(
                db, currentSql, currentRowBatch, status);
        if (status.getCode() != 0 || resultSet == null) {
            throw new SQLException("execute sql fail: " + status.getMsg());
        }
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
        Status status = new Status();
        this.currentRow = router.GetRequestRow(db, currentSql, status);
        if (status.getCode() != 0 || this.currentRow == null) {
            logger.error("getRequestRow failed: {}", status.getMsg());
            throw new SQLException("getRequestRow failed!, msg: " + status.getMsg());
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
        this.commonColumnIndices = null;
        this.currentRowBatch = null;
    }
}
