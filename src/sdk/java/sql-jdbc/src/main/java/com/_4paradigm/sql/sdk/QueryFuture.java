package com._4paradigm.sql.sdk;

import com._4paradigm.sql.Status;
import com._4paradigm.sql.jdbc.SQLResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class QueryFuture implements Future<java.sql.ResultSet>{
    private static final Logger logger = LoggerFactory.getLogger(QueryFuture.class);
    com._4paradigm.sql.QueryFuture queryFuture;

    public QueryFuture(com._4paradigm.sql.QueryFuture queryFuture) {
        this.queryFuture = queryFuture;
    }

    @Override
    @Deprecated
    public boolean cancel(boolean b) {
        return false;
    }

    @Override
    @Deprecated
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return queryFuture.IsDone();
    }

    @Override
    public java.sql.ResultSet get() throws InterruptedException, ExecutionException {
        Status status = new Status();
        com._4paradigm.sql.ResultSet resultSet = queryFuture.GetResultSet(status);
        if (status.getCode() != 0 || resultSet == null) {
            String msg = status.getMsg();
            status.delete();
            status = null;
            logger.error("call procedure failed: {}", msg);
            throw new ExecutionException(new SqlException("call procedure failed: " + msg));
        }
        status.delete();
        status = null;
        return new SQLResultSet(resultSet, queryFuture);
    }

    /**
     *
     * @param l  current timeout set by executeQeuryAsyn, so the param is invalid
     * @param timeUnit
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    @Override
    @Deprecated
    public java.sql.ResultSet get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }
}
