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
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class BatchSQLExecutor extends BaseSQLExecutor {

    public BatchSQLExecutor(SqlExecutor executor, SQLCase sqlCase, SQLCaseType executorType) {
        super(executor, sqlCase, executorType);
    }
    public BatchSQLExecutor(SQLCase sqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> openMLDBInfoMap, SQLCaseType executorType) {
        super(sqlCase, executor, executorMap, openMLDBInfoMap, executorType);
    }

    @Override
    public boolean verify() {
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("hybridse-only")) {
            log.info("skip case in batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("batch-unsupport")) {
            log.info("skip case in batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-batch-unsupport")) {
            log.info("skip case in rtidb batch mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-unsupport")) {
            log.info("skip case in rtidb mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("performance-sensitive-unsupport")) {
            log.info("skip case in rtidb mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && !OpenMLDBGlobalVar.tableStorageMode.equals("memory") && sqlCase.getMode().contains("disk-unsupport")) {
            log.info("skip case in disk mode: {}", sqlCase.getDesc());
            return false;
        }
        if (OpenMLDBConfig.isCluster() && null != sqlCase.getMode() && sqlCase.getMode().contains("cluster-unsupport")) {
            log.info("skip case in cluster mode: {}", sqlCase.getDesc());
            return false;
        }
        return true;
    }

    @Override
    public void prepare(String version,SqlExecutor executor){

        sdkClient.createAndUseDB(dbName);
        sdkClient.setOnline();
        boolean useFirstInputAsRequests = false;
        List<InputDesc> inputs = sqlCase.getInputs();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                InputDesc input = inputs.get(i);
                if (StringUtils.isNotEmpty(input.getDb())) {
                    sdkClient.createAndUseDB(input.getDb());
                }else{
                    sdkClient.useDB(dbName);
                }
                String tableName = input.getName();
                String createSql = input.extractCreate();
                if(StringUtils.isEmpty(createSql)){
                    continue;
                }
                createSql = SQLCase.formatSql(createSql, i, tableName);
                createSql = SQLUtil.formatSql(createSql, OpenMLDBGlobalVar.mainInfo);
                sdkClient.execute(createSql);
                if (useFirstInputAsRequests && i==0) {
                    continue;
                }
                List<String> inserts = input.extractInserts();
                for (String insertSql : inserts) {
                    insertSql = SQLCase.formatSql(insertSql, i, tableName);
                    if (StringUtils.isNotEmpty(insertSql)) {
                        OpenMLDBResult openMLDBResult = sdkClient.execute(insertSql);
                        if (!openMLDBResult.isOk()) {
                            throw new RuntimeException("prepare insert fail, version:"+version+",openMLDBResult:"+openMLDBResult);
                        }
                    }
                }
            }
        }

    }

    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor){
        sdkClient.useDB(dbName);
        OpenMLDBResult openMLDBResult = null;
        List<String> sqls = sqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                    sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                }else {
                    sql = SQLUtil.formatSql(sql, tableNames);
                }
//                openMLDBResult = SDKUtil.sql(executor, dbName, sql);
                openMLDBResult = sdkClient.execute(sql);
            }
        }
        String sql = sqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            // log.info("sql:{}", sql);
            if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
            }else {
                sql = SQLUtil.formatSql(sql, tableNames);
            }
//            openMLDBResult = SDKUtil.sql(executor, dbName, sql);
            openMLDBResult = sdkClient.execute(sql);
        }
        return openMLDBResult;
    }
}
