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
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.openmldb.SDKClient;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com._4paradigm.openmldb.test_common.util.SDKUtil.createTable;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class JobExecutor extends BaseSQLExecutor {
    private SDKClient sdkClient;
    private boolean useFirstInputAsRequests = false;

    public JobExecutor(SqlExecutor executor, SQLCase sqlCase, SQLCaseType executorType) {
        super(executor, sqlCase, executorType);
        sdkClient = SDKClient.of(executor);
    }
    public JobExecutor(SQLCase sqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap, SQLCaseType executorType) {
        super(sqlCase, executor, executorMap, fedbInfoMap, executorType);
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
        log.info("version:{} prepare begin",version);
        sdkClient.createAndUseDB(dbName);
        sdkClient.execute("SET @@global.sync_job = 'true';");
        sdkClient.execute("SET @@global.job_timeout = '600000';");
        sdkClient.setOnline();
        // Create inputs' databasess if exist
//        HashSet<String> dbNames = new HashSet<>();
//        if (!StringUtils.isEmpty(dbName)) {
//            dbNames.add(dbName);
//        }
        List<InputDesc> inputs = sqlCase.getInputs();
//        if (!Objects.isNull(inputs)) {
//            for (InputDesc input : inputs) {
//                // CreateDB if input's db has been configured and hasn't been created before
//                if (StringUtils.isNotEmpty(input.getDb())) {
//                    sdkClient.createAndUseDB(input.getDb());
//                    OpenMLDBGlobalVar.CREATE_DB_NAMES.add(input.getDb());
//                }
//            }
//        }

//        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
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
//                String dbName = inputs.get(i).getDb().isEmpty() ? this.dbName : inputs.get(i).getDb();
//                createTable(executor,dbName,createSql);
                sdkClient.execute(createSql);
//                InputDesc input = inputs.get(i);
                if (useFirstInputAsRequests && i==0) {
                    continue;
                }
                List<String> inserts = input.extractInserts();
                for (String insertSql : inserts) {
                    insertSql = SQLCase.formatSql(insertSql, i, tableName);
                    if (StringUtils.isNotEmpty(insertSql)) {
                        OpenMLDBResult openMLDBResult = sdkClient.execute(insertSql);
//                        OpenMLDBResult res = SDKUtil.insert(executor, dbName, insertSql);
                        if (!openMLDBResult.isOk()) {
                            throw new RuntimeException("prepare insert fail, version:"+version+",openMLDBResult:"+openMLDBResult);
                        }
                    }
                }
            }
        }
//        openMLDBResult.setOk(true);
//        return openMLDBResult;
//        OpenMLDBResult res = SDKUtil.createAndInsert(executor, dbName, sqlCase.getInputs(), false);
//        if (!res.isOk()) {
//            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail . version:"+version);
//        }
        log.info("version:{} prepare end",version);
    }

    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor){
        log.info("version:{} execute begin",version);
        sdkClient.useDB(dbName);
        OpenMLDBResult openMLDBResult = null;
        List<String> sqls = sqlCase.getSqls();
        if (CollectionUtils.isNotEmpty(sqls)) {
            for (String sql : sqls) {
                sql = MapUtils.isNotEmpty(openMLDBInfoMap)?SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version)):SQLUtil.formatSql(sql, tableNames);
                sql = SQLUtil.formatSql(sql);
                openMLDBResult = sdkClient.execute(sql);
            }
        }
        String sql = sqlCase.getSql();
        if (StringUtils.isNotEmpty(sql)) {
            sql = MapUtils.isNotEmpty(openMLDBInfoMap)?SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version)):SQLUtil.formatSql(sql, tableNames);
            sql = SQLUtil.formatSql(sql);
            openMLDBResult = sdkClient.execute(sql);
        }
        log.info("version:{} execute end",version);
        return openMLDBResult;
    }
}
