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
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
public class ExecutorFactory {

    public static IExecutor build(SQLCase sqlCase, SQLCaseType type) {
        switch (type){
            case kSQLITE3:
                return new Sqlite3Executor(sqlCase,type);
            case kMYSQL:
                return new MysqlExecutor(sqlCase,type);
            case kCLI:
                return new CommandExecutor(sqlCase,type);
            case kStandaloneCLI:
                return new StandaloneCliExecutor(sqlCase,type);
            case kClusterCLI:
                return new ClusterCliExecutor(sqlCase,type);
        }
        return null;
    }

    public static IExecutor build(SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap, SQLCase sqlCase, SQLCaseType type) {
        switch (type) {
            case kDiffBatch: {
                return new BatchSQLExecutor(sqlCase, executor, executorMap, fedbInfoMap, type);
            }
            case kDiffRequest:{
                return new RequestQuerySQLExecutor(sqlCase, executor, executorMap, fedbInfoMap, false, false, type);
            }
            case kDiffRequestWithSp:{
                return new StoredProcedureSQLExecutor(sqlCase, executor, executorMap, fedbInfoMap, false, false, type);
            }
            case kDiffRequestWithSpAsync:{
                return new StoredProcedureSQLExecutor(sqlCase, executor, executorMap, fedbInfoMap, false, true, type);
            }
        }
        return null;
    }
    public static BaseSQLExecutor build(SqlExecutor executor, SQLCase sqlCase, SQLCaseType type) {
        switch (type) {
            case kDDL: {
                return getDDLExecutor(executor, sqlCase, type);
            }
            case kInsertPrepared: {
                return new InsertPreparedExecutor(executor,sqlCase,type);
            }
            case kSelectPrepared: {
                return new QueryPreparedExecutor(executor,sqlCase,type);
            }
            case kBatch: {
                return getFeBatchQueryExecutor(executor, sqlCase, type);
            }
            case kRequest: {
                return getFeRequestQueryExecutor(executor, sqlCase, type);
            }
            case kBatchRequest: {
                return getFeBatchRequestQueryExecutor(executor, sqlCase, type);
            }
            case kRequestWithSp: {
                return getFeRequestQueryWithSpExecutor(executor, sqlCase, false, type);
            }
            case kRequestWithSpAsync: {
                return getFeRequestQueryWithSpExecutor(executor, sqlCase, true, type);
            }
            case kBatchRequestWithSp: {
                return getFeBatchRequestQueryWithSpExecutor(executor, sqlCase, false, type);
            }
            case kBatchRequestWithSpAsync: {
                return getFeBatchRequestQueryWithSpExecutor(executor, sqlCase, true, type);
            }
            case kDiffSQLResult:
                return new DiffResultExecutor(executor,sqlCase,type);
            case kLongWindow:
                return new LongWindowExecutor(executor,sqlCase,false,false,type);
            case kJob:
                return new JobExecutor(executor,sqlCase,type);
        }
        return null;
    }
    private static BaseSQLExecutor getDDLExecutor(SqlExecutor sqlExecutor, SQLCase sqlCase, SQLCaseType type) {
        return new BatchSQLExecutor(sqlExecutor, sqlCase, type);
    }
    private static BaseSQLExecutor getFeBatchQueryExecutor(SqlExecutor sqlExecutor, SQLCase sqlCase, SQLCaseType type) {
        return new BatchSQLExecutor(sqlExecutor, sqlCase, type);
    }
    private static BaseSQLExecutor getFeRequestQueryExecutor(SqlExecutor sqlExecutor, SQLCase sqlCase, SQLCaseType type) {
        return new RequestQuerySQLExecutor(sqlExecutor, sqlCase, false, false, type);
    }

    private static BaseSQLExecutor getFeBatchRequestQueryExecutor(SqlExecutor sqlExecutor,
                                                                  SQLCase sqlCase, SQLCaseType type) {
        return new RequestQuerySQLExecutor(
                sqlExecutor, sqlCase, true, false, type);
    }

    private static BaseSQLExecutor getFeRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase sqlCase, boolean isAsyn, SQLCaseType type) {
        return new StoredProcedureSQLExecutor(
                sqlExecutor, sqlCase, false, isAsyn, type);
    }
    private static BaseSQLExecutor getFeBatchRequestQueryWithSpExecutor(SqlExecutor sqlExecutor, SQLCase sqlCase, boolean isAsyn, SQLCaseType type) {
        return new StoredProcedureSQLExecutor(
                sqlExecutor, sqlCase, sqlCase.getBatch_request() != null, isAsyn, type);
    }
}
