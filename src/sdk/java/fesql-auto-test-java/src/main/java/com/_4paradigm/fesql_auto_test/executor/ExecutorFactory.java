package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.executor.diff.DiffStoredProcedureExecutor;
import com._4paradigm.fesql_auto_test.executor.diff.DiffVersionRequestExecutor;
import com._4paradigm.fesql_auto_test.executor.diff.DiffVersionSQLExecutor;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ExecutorFactory {

    public enum ExecutorType {
        kDDL,                       //执行DDL
        kBatch,                     //在线批量查询
        kRequest,                   //请求模式
        kBatchRequest,              //批量请求模式
        kRequestWithSp,             //
        kRequestWithSpAsync,
        kBatchRequestWithSp,
        kBatchRequestWithSpAsync,
        kDiffBatch,
        kDiffRequest,
        kDiffRequestWithSp,
        kDiffRequestWithSpAsync,
    }
    public static IExecutor build(SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap, SQLCase fesqlCase, ExecutorType type) {
        switch (type) {
            case kDiffBatch: {
                return new DiffVersionSQLExecutor(fesqlCase, executor, executorMap, fedbInfoMap);
            }
            case kDiffRequest:{
                return new DiffVersionRequestExecutor(fesqlCase, executor, executorMap, fedbInfoMap, false, false);
            }
            case kDiffRequestWithSp:{
                return new DiffStoredProcedureExecutor(fesqlCase, executor, executorMap, fedbInfoMap, false, false);
            }
            case kDiffRequestWithSpAsync:{
                return new DiffStoredProcedureExecutor(fesqlCase, executor, executorMap, fedbInfoMap, false, true);
            }
        }
        return null;
    }
    public static BaseExecutor build(SqlExecutor executor, SQLCase fesqlCase, ExecutorType type) {
        switch (type) {
            case kDDL: {
                return getDDLExecutor(executor, fesqlCase);
            }
            case kBatch: {
                return getFeBatchQueryExecutor(executor, fesqlCase);
            }
            case kRequest: {
                return getFeRequestQueryExecutor(executor, fesqlCase);
            }
            case kBatchRequest: {
                return getFeBatchRequestQueryExecutor(executor, fesqlCase);
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
    private static BaseExecutor getDDLExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new SQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }
    private static BaseExecutor getFeBatchQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        if (FesqlConfig.isCluster()) {
            log.info("cluster unsupport batch query mode");
            return new NullExecutor(sqlExecutor, fesqlCase);
        }
        BaseExecutor executor = null;
        executor = new SQLExecutor(sqlExecutor, fesqlCase);
        return executor;
    }
    private static BaseExecutor getFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase) {
        BaseExecutor executor = null;
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase, false, false);
        return executor;
    }

    private static BaseExecutor getFeBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                               SQLCase fesqlCase) {
        RequestQuerySQLExecutor executor = new RequestQuerySQLExecutor(
                sqlExecutor, fesqlCase, true, false);
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
}
