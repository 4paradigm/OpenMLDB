package com._4paradigm.sql.sdk.impl;

import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.Status;
import com._4paradigm.sql.jdbc.RequestPreparedStatement;

import java.sql.SQLException;
import java.util.ArrayList;

public class RequestPreparedStatementImpl extends RequestPreparedStatement {

    public RequestPreparedStatementImpl(String db, String sql, SQLRouter router) throws SQLException {
        if (db == null) throw new SQLException("db is null");
        if (router == null) throw new SQLException("router is null");
        if (sql == null) throw new SQLException("spName is null");
        this.db = db;
        this.currentSql = sql;
        this.router = router;
        Status status = new Status();
        this.currentRow = router.GetRequestRow(db, sql, status);
        if (status.getCode() != 0 || this.currentRow == null) {
            String msg = status.getMsg();
            status.delete();
            if (currentRow != null) {
                currentRow.delete();
                currentRow = null;
            }
            throw new SQLException("get GetRequestRow fail " + msg + " in construction preparedstatement");
        }
        status.delete();
        this.currentSchema = this.currentRow.GetSchema();
        int cnt = this.currentSchema.GetColumnCnt();
        this.currentDatas = new ArrayList<>(cnt);
        this.hasSet = new ArrayList<>(cnt);
        for (int i = 0; i < cnt; i++) {
            this.hasSet.add(false);
            currentDatas.add(null);
        }
    }
}
