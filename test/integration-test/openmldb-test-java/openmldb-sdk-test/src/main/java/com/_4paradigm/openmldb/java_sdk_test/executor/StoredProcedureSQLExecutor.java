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

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.util.OpenMLDBUtil;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class StoredProcedureSQLExecutor extends RequestQuerySQLExecutor {

    private List<String> spNames;

    public StoredProcedureSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(executor, fesqlCase, isBatchRequest, isAsyn, executorType);
        spNames = new ArrayList<>();
    }

    public StoredProcedureSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap, boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, isBatchRequest, isAsyn, executorType);
        spNames = new ArrayList<>();
    }

    @Override
    public void prepare(String version,SqlExecutor executor){
        logger.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        logger.info("create db:{},{}", dbName, dbOk);
        OpenMLDBResult res = OpenMLDBUtil.createAndInsert(
                executor, dbName, fesqlCase.getInputs(),
                !isBatchRequest && null == fesqlCase.getBatch_request());
        if (!res.isOk()) {
            throw new RuntimeException("fail to run StoredProcedureSQLExecutor: prepare fail");
        }
        logger.info("version:{} prepare end",version);
    }
    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor) {
        logger.info("version:{} execute begin",version);
        OpenMLDBResult fesqlResult = null;
        try {
            if (fesqlCase.getInputs().isEmpty() ||
                    CollectionUtils.isEmpty(fesqlCase.getInputs().get(0).getRows())) {
                logger.error("fail to execute in request query sql executor: sql case inputs is empty");
                return null;
            }
            String sql = fesqlCase.getSql();
            logger.info("sql: {}", sql);
            if (sql == null || sql.length() == 0) {
                return null;
            }
            if (fesqlCase.getBatch_request() != null) {
                fesqlResult = executeBatch(executor, sql, this.isAsyn);
            } else {
                fesqlResult = executeSingle(executor, sql, this.isAsyn);
            }
            spNames.add(fesqlCase.getSpName());
        }catch (Exception e){
            e.printStackTrace();
        }
        logger.info("version:{} execute end",version);
        return fesqlResult;
    }

    private OpenMLDBResult executeSingle(SqlExecutor executor, String sql, boolean isAsyn) throws SQLException {
        String spSql = fesqlCase.getProcedure(sql);
        logger.info("spSql: {}", spSql);
        return OpenMLDBUtil.sqlRequestModeWithSp(
                executor, dbName, fesqlCase.getSpName(), null == fesqlCase.getBatch_request(),
                spSql, fesqlCase.getInputs().get(0), isAsyn);
    }

    private OpenMLDBResult executeBatch(SqlExecutor executor, String sql, boolean isAsyn) throws SQLException {
        String spName = "sp_" + tableNames.get(0) + "_" + System.currentTimeMillis();
        String spSql = OpenMLDBUtil.buildSpSQLWithConstColumns(spName, sql, fesqlCase.getBatch_request());
        logger.info("spSql: {}", spSql);
        return OpenMLDBUtil.selectBatchRequestModeWithSp(
                executor, dbName, spName, spSql, fesqlCase.getBatch_request(), isAsyn);
    }


    @Override
    public void tearDown(String version,SqlExecutor executor) {
        logger.info("version:{},begin drop table",version);
        if (CollectionUtils.isEmpty(spNames)) {
            return;
        }
        for (String spName : spNames) {
            String drop = "drop procedure " + spName + ";";
//            FesqlUtil.ddl(executor, dbName, drop);
        }
        super.tearDown(version,executor);
    }
}
