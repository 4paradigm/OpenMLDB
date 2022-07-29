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

package com._4paradigm.openmldb.java_sdk_test.executor;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBConfig;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.common.ReportLog;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ExecutorFactory {

    private static ReportLog reportLog = ReportLog.of();

    public static IExecutor build(SQLCase fesqlCase, SQLCaseType type) {
        switch (type){
            case kSQLITE3:
                return new Sqlite3Executor(fesqlCase,type);
            case kMYSQL:
                return new MysqlExecutor(fesqlCase,type);
            case kCLI:
                return new CommandExecutor(fesqlCase,type);
            case kStandaloneCLI:
                return new StandaloneCliExecutor(fesqlCase,type);
            case kClusterCLI:
                return new ClusterCliExecutor(fesqlCase,type);
        }
        return null;
    }

    public static IExecutor build(SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap, SQLCase fesqlCase, SQLCaseType type) {
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
    public static BaseSQLExecutor build(SqlExecutor executor, SQLCase fesqlCase, SQLCaseType type) {
        switch (type) {
            case kDDL: {
                return getDDLExecutor(executor, fesqlCase, type);
            }
            case kInsertPrepared: {
                return new InsertPreparedExecutor(executor,fesqlCase,type);
            }
            case kSelectPrepared: {
                return new QueryPreparedExecutor(executor,fesqlCase,type);
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
            case kDiffSQLResult:
                return new DiffResultExecutor(executor,fesqlCase,type);
        }
        return null;
    }
    private static BaseSQLExecutor getDDLExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, SQLCaseType type) {
        BaseSQLExecutor executor = null;
        executor = new BatchSQLExecutor(sqlExecutor, fesqlCase, type);
        return executor;
    }
    private static BaseSQLExecutor getFeBatchQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, SQLCaseType type) {
        if (OpenMLDBConfig.isCluster()) {
            log.info("cluster unsupport batch query mode");
            reportLog.info("cluster unsupport batch query mode");
            return new NullExecutor(sqlExecutor, fesqlCase, type);
        }
        BaseSQLExecutor executor = null;
        executor = new BatchSQLExecutor(sqlExecutor, fesqlCase, type);
        return executor;
    }
    private static BaseSQLExecutor getFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, SQLCaseType type) {
        BaseSQLExecutor executor = null;
        executor = new RequestQuerySQLExecutor(sqlExecutor, fesqlCase, false, false, type);
        return executor;
    }

    private static BaseSQLExecutor getFeBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                                  SQLCase fesqlCase, SQLCaseType type) {
        RequestQuerySQLExecutor executor = new RequestQuerySQLExecutor(
                sqlExecutor, fesqlCase, true, false, type);
        return executor;
    }

    private static BaseSQLExecutor getFeRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn, SQLCaseType type) {
        BaseSQLExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, false, isAsyn, type);
        return executor;
    }
    private static BaseSQLExecutor getFeBatchRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase fesqlCase, boolean isAsyn, SQLCaseType type) {
        BaseSQLExecutor executor = null;
        executor = new StoredProcedureSQLExecutor(
                sqlExecutor, fesqlCase, fesqlCase.getBatch_request() != null, isAsyn, type);
        return executor;
    }
}
