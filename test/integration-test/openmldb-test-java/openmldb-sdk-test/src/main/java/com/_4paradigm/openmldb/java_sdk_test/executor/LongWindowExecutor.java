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

import com._4paradigm.openmldb.java_sdk_test.checker.Checker;
import com._4paradigm.openmldb.java_sdk_test.checker.CheckerStrategy;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class LongWindowExecutor extends StoredProcedureSQLExecutor {

//    private List<String> spNames;

    public LongWindowExecutor(SqlExecutor executor, SQLCase sqlCase, boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(executor, sqlCase, isBatchRequest, isAsyn, executorType);
        spNames = new ArrayList<>();
    }

    public LongWindowExecutor(SQLCase sqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> openMLDBInfoMap, boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(sqlCase, executor, executorMap, openMLDBInfoMap, isBatchRequest, isAsyn, executorType);
        spNames = new ArrayList<>();
    }

    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor) {
        log.info("version:{} execute begin",version);
        OpenMLDBResult openMLDBResult = null;
        try {
            List<SQLCase> steps = sqlCase.getSteps();
            if(CollectionUtils.isNotEmpty(steps)) {
                for (SQLCase step : steps) {
                    String sql = step.getSql();
                    if (MapUtils.isNotEmpty(openMLDBInfoMap)) {
                        sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                    } else {
                        sql = SQLUtil.formatSql(sql, tableNames);
                    }
                    if(sql.toLowerCase().startsWith("select ")) {
                        openMLDBResult = SDKUtil.executeLongWindowDeploy(executor, sqlCase, sql, false);
                        openMLDBResult.setDbName(dbName);
                        spNames.add(sqlCase.getSpName());
                    }else{
                        openMLDBResult = SDKUtil.sql(executor, dbName, sql);
                        openMLDBResult.setDbName(dbName);
                        openMLDBResult.setSpName(spNames.get(0));
                    }
//                    if (executorType == SQLCaseType.kRequest) {
//                        InputDesc request = sqlCase.getInputs().get(0);
//                        openMLDBResult = SDKUtil.sqlRequestMode(executor, dbName, true, sql, request);
//                    } else if (executorType == SQLCaseType.kLongWindow) {
//                        openMLDBResult = SDKUtil.executeLongWindowDeploy(executor, sqlCase, sql, false);
//                        spNames.add(sqlCase.getSpName());
//                    } else {
//                        openMLDBResult = SDKUtil.sql(executor, dbName, sql);
//                    }
                    List<Checker> strategyList = CheckerStrategy.build(executor, step, openMLDBResult, executorType);
                    for (Checker checker : strategyList) {
                        checker.check();
                    }
                }
            }else {
                if (sqlCase.getInputs().isEmpty() ||
                        CollectionUtils.isEmpty(sqlCase.getInputs().get(0).getRows())) {
                    log.error("fail to execute in request query sql executor: sql case inputs is empty");
                    return null;
                }
                String sql = sqlCase.getSql();
                log.info("sql: {}", sql);
                if (sql == null || sql.length() == 0) {
                    return null;
                }
                openMLDBResult = SDKUtil.executeLongWindowDeploy(executor, sqlCase, this.isAsyn);
                spNames.add(sqlCase.getSpName());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("version:{} execute end",version);
        return openMLDBResult;
    }

//    private OpenMLDBResult executeSingle(SqlExecutor executor, String sql, boolean isAsyn) throws SQLException {
//        String spSql = sqlCase.getProcedure(sql);
//        log.info("spSql: {}", spSql);
//        return SDKUtil.sqlRequestModeWithProcedure(
//                executor, dbName, sqlCase.getSpName(), null == sqlCase.getBatch_request(),
//                spSql, sqlCase.getInputs().get(0), isAsyn);
//    }


//    @Override
//    public void tearDown(String version,SqlExecutor executor) {
//        log.info("version:{},begin tearDown",version);
//        if (CollectionUtils.isEmpty(spNames)) {
//            return;
//        }
//        for (String spName : spNames) {
//            String drop = "drop procedure " + spName + ";";
//            SDKUtil.ddl(executor, dbName, drop);
//        }
//        super.tearDown(version,executor);
//    }
}
