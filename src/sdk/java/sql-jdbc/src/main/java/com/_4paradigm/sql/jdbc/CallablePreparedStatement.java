package com._4paradigm.sql.jdbc;

import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;

public class CallablePreparedStatement extends RequestPreparedStatement{
    private static final Logger logger = LoggerFactory.getLogger(CallablePreparedStatement.class);
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
            throw new SQLException("show procedure failed, msg: " + status.getMsg());
        }
        this.currentSql = procedureInfo.GetSql();
        this.currentRow = router.GetRequestRow(db, procedureInfo.GetSql(), status);
        if (status.getCode() != 0 || this.currentRow == null) {
            logger.error("getRequestRow failed: {}", status.getMsg());
            throw new SQLException("getRequestRow failed!, msg: " + status.getMsg());
        }
        this.currentSchema = procedureInfo.GetInputSchema();
        if (this.currentSchema == null) {
            throw new SQLException("inputSchema is null");
        }
        int cnt = this.currentSchema.GetColumnCnt();
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
}
