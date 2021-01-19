package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.Status;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class CallablePreparedStatement extends RequestPreparedStatement {
    protected String spName;

    public CallablePreparedStatement(String db, String spName, SQLRouter router) throws SQLException {
        if (db == null) throw new SQLException("db is null");
        if (router == null) throw new SQLException("router is null");
        if (spName == null) throw new SQLException("spName is null");

        this.db = db;
        this.router = router;
        this.spName = spName;

        Status status = new Status();
        com._4paradigm.sql.ProcedureInfo procedureInfo = router.ShowProcedure(db, spName, status);
        if (procedureInfo == null || status.getCode() != 0) {
            String msg = status.getMsg();
            status.delete();
            if (procedureInfo != null) {
                procedureInfo.delete();
            }
            throw new SQLException("show procedure failed, msg: " + msg);
        }
        this.currentSql = procedureInfo.GetSql();
        this.currentRow = router.GetRequestRow(db, procedureInfo.GetSql(), status);
        if (status.getCode() != 0 || this.currentRow == null) {
            String msg = status.getMsg();
            status.delete();
            status = null;
            procedureInfo.delete();
            throw new SQLException("getRequestRow failed!, msg: " + msg);
        }
        status.delete();
        status = null;
        this.currentSchema = procedureInfo.GetInputSchema();
        if (this.currentSchema == null) {
            procedureInfo.delete();
            throw new SQLException("inputSchema is null");
        }
        int cnt = this.currentSchema.GetColumnCnt();
        procedureInfo.delete();
        this.currentDatas = new ArrayList<>(cnt);
        this.hasSet = new ArrayList<>(cnt);
        for (int i = 0; i < cnt; i++) {
            this.hasSet.add(false);
            currentDatas.add(null);
        }
    }

    @Override
    public void close() throws SQLException {
        super.close();
        this.spName = null;
    }

    public com._4paradigm.sql.sdk.QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        throw new SQLException("current do not support this method");
    }
}
