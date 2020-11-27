package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.sql.sdk.SqlExecutor;
public class ExecutorFactory {

    public enum ExecutorType {
        kGenenal,
        kBatch,
        kRequest,
        kBatchRequest,
        kClusterRequest,
        kClusterBatchRequest,
        kRequestWithSp,
        kRequestWithSpAsync,
        kBatchRequestWithSp,
        kBatchRequestWithSpAsync,
    }
    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase, ExecutorType type) {
        switch (type) {
            case kGenenal: {
                return getGeneratorExecutor(executor, fesqlCase);
            }
            case kBatch: {
                return getFeExecutor(executor, fesqlCase);
            }
            case kRequest: {
                return getFeRequestQueryExecutor(executor, fesqlCase);
            }
            case kBatchRequest: {
                return getFeBatchRequestQueryExecutor(executor, fesqlCase);
            }
            case kClusterRequest: {
                return getClusterFeRequestQueryExecutor(executor, fesqlCase);
            }
            case kClusterBatchRequest: {
                return getClusterFeBatchRequestQueryExecutor(executor, fesqlCase);
            }
            case kRequestWithSp: {
                return getFeRequestQueryWithSpExecutor(executor, fesqlCase, false);
            }
            case kRequestWithSpAsync: {
                return getFeRequestQueryWithSpExecutor(executor, fesqlCase, true);
            }
            case kBatchRequestWithSp: {
                return getFeBatchRequestQueryWithSpExecutor(executor, fesqlCase, false);
            }
            case kBatchRequestWithSpAsync: {
                return getFeBatchRequestQueryWithSpExecutor(executor, fesqlCase, true);
            }

        }
        return null;
    }
    private static BaseExecutor getGeneratorExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new SQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }
    private static BaseExecutor getFeExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new SQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }
    private static BaseExecutor getFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase, false, false, false);
        return executor;
    }

    private static BaseExecutor getFeBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                               SQLCase fesqlCase) {
        RequestQuerySQLExecutor executor = new RequestQuerySQLExecutor(
                sqlExecutor, fesqlCase, true, false, false);
        return executor;
    }

    private static BaseExecutor getFeRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn) {
        BaseExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, false, isAsyn);
        return executor;
    }
    private static BaseExecutor getFeBatchRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn) {
        BaseExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, fesqlCase.getBatch_request() != null, isAsyn);
        return executor;
    }


    private static BaseExecutor getClusterFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase, false, true, false);
        return executor;
    }

    private static BaseExecutor getClusterFeBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                               SQLCase fesqlCase) {
        RequestQuerySQLExecutor executor = new RequestQuerySQLExecutor(
                sqlExecutor, fesqlCase, true, true, false);
        return executor;
    }
}
