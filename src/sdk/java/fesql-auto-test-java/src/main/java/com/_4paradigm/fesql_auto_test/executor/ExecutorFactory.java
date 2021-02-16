package com._4paradigm.fesql_auto_test.executor;


import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.util.ReportLog;
import com._4paradigm.sql.sdk.SqlExecutor;

import java.util.Map;

// @Slf4j
public class ExecutorFactory {

    private static ReportLog log = ReportLog.of();

    public enum ExecutorType {
        kDDL("DDL"),                       //执行DDL
        kBatch("BATCH"),                     //在线批量查询
        kRequest("REQUEST"),                   //请求模式
        kBatchRequest("BATCH_REQUEST"),              //批量请求模式
        kRequestWithSp("REQUEST_WITH_SP"),             //
        kRequestWithSpAsync("REQUEST_WITH_SP_ASYNC"),
        kBatchRequestWithSp("BATCH_REQUEST_WITH_SP"),
        kBatchRequestWithSpAsync("BATCH_REQUEST_WITH_SP_ASYNC"),
        kDiffBatch("DIFF_BATCH"),
        kDiffRequest("DIFF_REQUEST"),
        kDiffRequestWithSp("DIFF_REQUEST_WITH_SP"),
        kDiffRequestWithSpAsync("DIFF_REQUEST_WITH_SP_ASYNC"),
        ;
        private String typeName;
        ExecutorType(String typeName){
            this.typeName = typeName;
        }
    }
    public static IExecutor build(SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap, SQLCase fesqlCase, ExecutorType type) {
        switch (type) {
            case kDiffBatch: {
                return new BatchSQLExecutor(fesqlCase, executor, executorMap, fedbInfoMap, type);
            }
            case kDiffRequest:{
                return new RequestQuerySQLExecutor(fesqlCase, executor, executorMap, fedbInfoMap, false, false, type);
            }
            case kDiffRequestWithSp:{
                return new StoredProcedureSQLExecutor(fesqlCase, executor, executorMap, fedbInfoMap, false, false, type);
            }
            case kDiffRequestWithSpAsync:{
                return new StoredProcedureSQLExecutor(fesqlCase, executor, executorMap, fedbInfoMap, false, true, type);
            }
        }
        return null;
    }
    public static BaseSQLExecutor build(SqlExecutor executor, SQLCase fesqlCase, ExecutorType type) {
        switch (type) {
            case kDDL: {
                return getDDLExecutor(executor, fesqlCase, type);
            }
            case kBatch: {
                return getFeBatchQueryExecutor(executor, fesqlCase, type);
            }
            case kRequest: {
                return getFeRequestQueryExecutor(executor, fesqlCase, type);
            }
            case kBatchRequest: {
                return getFeBatchRequestQueryExecutor(executor, fesqlCase, type);
            }
            case kRequestWithSp: {
                return getFeRequestQueryWithSpExecutor(executor, fesqlCase, false, type);
            }
            case kRequestWithSpAsync: {
                return getFeRequestQueryWithSpExecutor(executor, fesqlCase, true, type);
            }
            case kBatchRequestWithSp: {
                return getFeBatchRequestQueryWithSpExecutor(executor, fesqlCase, false, type);
            }
            case kBatchRequestWithSpAsync: {
                return getFeBatchRequestQueryWithSpExecutor(executor, fesqlCase, true, type);
            }

        }
        return null;
    }
    private static BaseSQLExecutor getDDLExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, ExecutorType type) {
        BaseSQLExecutor executor = null;
        executor = new BatchSQLExecutor(sqlExecutor, fesqlCase, type);
        return executor;
    }
    private static BaseSQLExecutor getFeBatchQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, ExecutorType type) {
        if (FesqlConfig.isCluster()) {
            log.info("cluster unsupport batch query mode");
            return new NullExecutor(sqlExecutor, fesqlCase, type);
        }
        BaseSQLExecutor executor = null;
        executor = new BatchSQLExecutor(sqlExecutor, fesqlCase, type);
        return executor;
    }
    private static BaseSQLExecutor getFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, ExecutorType type) {
        BaseSQLExecutor executor = null;
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase, false, false, type);
        return executor;
    }

    private static BaseSQLExecutor getFeBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                                  SQLCase fesqlCase, ExecutorType type) {
        RequestQuerySQLExecutor executor = new RequestQuerySQLExecutor(
                sqlExecutor, fesqlCase, true, false, type);
        return executor;
    }

    private static BaseSQLExecutor getFeRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn, ExecutorType type) {
        BaseSQLExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, false, isAsyn, type);
        return executor;
    }
    private static BaseSQLExecutor getFeBatchRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn, ExecutorType type) {
        BaseSQLExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, fesqlCase.getBatch_request() != null, isAsyn, type);
        return executor;
    }
}
