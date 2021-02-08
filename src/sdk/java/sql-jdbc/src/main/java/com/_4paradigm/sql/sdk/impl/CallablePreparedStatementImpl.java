package com._4paradigm.sql.sdk.impl;

import com._4paradigm.sql.QueryFuture;
import com._4paradigm.sql.SQLRouter;
import com._4paradigm.sql.Status;
import com._4paradigm.sql.jdbc.CallablePreparedStatement;
import com._4paradigm.sql.jdbc.SQLResultSet;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class CallablePreparedStatementImpl extends CallablePreparedStatement {

    public CallablePreparedStatementImpl(String db, String spName, SQLRouter router) throws SQLException {
        super(db, spName, router);
    }

    @Override
    public SQLResultSet executeQuery() throws SQLException {
        checkClosed();
        dataBuild();
        Status status = new Status();
        com._4paradigm.sql.ResultSet resultSet = router.CallProcedure(db, spName, currentRow, status);
        if (status.getCode() != 0 || resultSet == null) {
            String msg = status.getMsg();
            status.delete();
            if (resultSet != null) {
                resultSet.delete();
            }
            throw new SQLException("call procedure fail, msg: " + msg);
        }
        status.delete();
        SQLResultSet rs = new SQLResultSet(resultSet);
        if (closeOnComplete) {
            closed = true;
        }
        return rs;
    }

    @Override
    public com._4paradigm.sql.sdk.QueryFuture executeQueryAsync(long timeOut, TimeUnit unit) throws SQLException {
        checkClosed();
        dataBuild();
        Status status = new Status();
        QueryFuture queryFuture = router.CallProcedure(db, spName, unit.toMillis(timeOut), currentRow, status);
        if (status.getCode() != 0 || queryFuture == null) {
            String msg = status.getMsg();
            status.delete();
            if (queryFuture != null) {
                queryFuture.delete();
            }
            throw new SQLException("call procedure fail, msg: " + msg);
        }
        status.delete();
        return new com._4paradigm.sql.sdk.QueryFuture(queryFuture);
    }

}
