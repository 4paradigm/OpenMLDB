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
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class RequestQuerySQLExecutor extends BaseSQLExecutor {

    protected boolean isBatchRequest;
    protected boolean isAsyn;

    public RequestQuerySQLExecutor(SqlExecutor executor, SQLCase sqlCase,
                                   boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(executor, sqlCase, executorType);
        this.isBatchRequest = isBatchRequest;
        this.isAsyn = isAsyn;
    }
    public RequestQuerySQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap,
                                      boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, executorType);
        this.isBatchRequest = isBatchRequest;
        this.isAsyn = isAsyn;
    }
    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor) {
        log.info("version:{} execute begin",version);
        OpenMLDBResult fesqlResult = null;
        try {
             List<String> sqls = sqlCase.getSqls();
             if (sqls != null && sqls.size() > 0) {
                 for (String sql : sqls) {
                     // log.info("sql:{}", sql);
                     if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                         sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
                     }else {
                         sql = SQLUtil.formatSql(sql, tableNames);
                     }
                     fesqlResult = SDKUtil.sql(executor, dbName, sql);
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
                InputDesc request = null;
                if (isBatchRequest) {
                    InputDesc batchRequest = sqlCase.getBatch_request();
                    if (batchRequest == null) {
                        log.error("No batch request provided in case");
                        return null;
                    }
                    List<Integer> commonColumnIndices = new ArrayList<>();
                    if (batchRequest.getCommon_column_indices() != null) {
                        for (String str : batchRequest.getCommon_column_indices()) {
                            if (str != null) {
                                commonColumnIndices.add(Integer.parseInt(str));
                            }
                        }
                    }

                    fesqlResult = SDKUtil.sqlBatchRequestMode(
                            executor, dbName, sql, batchRequest, commonColumnIndices);
                } else {
                    if (null != sqlCase.getBatch_request()) {
                        request = sqlCase.getBatch_request();
                    } else if (!sqlCase.getInputs().isEmpty()) {
                        request = sqlCase.getInputs().get(0);
                    }
                    if (null == request || CollectionUtils.isEmpty(request.getColumns())) {
                        log.error("fail to execute in request query sql executor: sql case request columns is empty");
                        return null;
                    }
                    fesqlResult = SDKUtil.sqlRequestMode(executor, dbName, null == sqlCase.getBatch_request(), sql, request);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("version:{} execute end",version);
        return fesqlResult;
    }

    @Override
    protected void prepare(String version,SqlExecutor executor) {
        log.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        SDKUtil.useDB(executor,dbName);
        boolean useFirstInputAsRequests = !isBatchRequest && null == sqlCase.getBatch_request();
        OpenMLDBResult res = SDKUtil.createAndInsert(executor, dbName, sqlCase.getInputs(), useFirstInputAsRequests);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run RequestQuerySQLExecutor: prepare fail");
        }
        log.info("version:{} prepare end",version);
    }

    @Override
    public boolean verify() {
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("hybridse-only")) {
            log.info("skip case in request mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("request-unsupport")) {
            log.info("skip case in request mode: {}", sqlCase.getDesc());
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
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-request-unsupport")) {
            log.info("skip case in rtidb request mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && !OpenMLDBGlobalVar.tableStorageMode.equals("memory") && sqlCase.getMode().contains("disk-unsupport")) {
            log.info("skip case in disk mode: {}", sqlCase.getDesc());
            return false;
        }
        if (null != sqlCase.getMode() && sqlCase.getMode().contains("procedure-unsupport")) {
            log.info("skip case in procedure mode: {}", sqlCase.getDesc());
            return false;
        }
        if (OpenMLDBConfig.isCluster() &&
                null != sqlCase.getMode() && sqlCase.getMode().contains("cluster-unsupport")) {
            log.info("cluster-unsupport, skip case in cluster request mode: {}", sqlCase.getDesc());
            return false;
        }
        return true;
    }
}
