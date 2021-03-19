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

package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.hybridse.sqlcase.model.InputDesc;
import com._4paradigm.hybridse.sqlcase.model.SQLCase;
import com._4paradigm.hybridse.sqlcase.model.SQLCaseType;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class RequestQuerySQLExecutor extends BaseSQLExecutor {

    protected boolean isBatchRequest;
    protected boolean isAsyn;

    public RequestQuerySQLExecutor(SqlExecutor executor, SQLCase fesqlCase,
                                   boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(executor, fesqlCase, executorType);
        this.isBatchRequest = isBatchRequest;
        this.isAsyn = isAsyn;
    }
    public RequestQuerySQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap,
                                      boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, executorType);
        this.isBatchRequest = isBatchRequest;
        this.isAsyn = isAsyn;
    }
    @Override
    public FesqlResult execute(String version, SqlExecutor executor) {
        log.info("version:{} execute begin",version);
        reportLog.info("version:{} execute begin",version);
        FesqlResult fesqlResult = null;
        try {
            // List<String> sqls = fesqlCase.getSqls();
            // if (sqls != null && sqls.size() > 0) {
            //     for (String sql : sqls) {
            //         // log.info("sql:{}", sql);
            //         if(MapUtils.isNotEmpty(fedbInfoMap)) {
            //             sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
            //         }else {
            //             sql = FesqlUtil.formatSql(sql, tableNames);
            //         }
            //         fesqlResult = FesqlUtil.sql(executor, dbName, sql);
            //     }
            // }
            String sql = fesqlCase.getSql();
            if (sql != null && sql.length() > 0) {
                // log.info("sql:{}", sql);
                if(MapUtils.isNotEmpty(fedbInfoMap)) {
                    sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                }else {
                    sql = FesqlUtil.formatSql(sql, tableNames);
                }
                InputDesc request = null;
                if (isBatchRequest) {
                    InputDesc batchRequest = fesqlCase.getBatch_request();
                    if (batchRequest == null) {
                        log.error("No batch request provided in case");
                        reportLog.error("No batch request provided in case");
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

                    fesqlResult = FesqlUtil.sqlBatchRequestMode(
                            executor, dbName, sql, batchRequest, commonColumnIndices);
                } else {
                    if (null != fesqlCase.getBatch_request()) {
                        request = fesqlCase.getBatch_request();
                    } else if (!fesqlCase.getInputs().isEmpty()) {
                        request = fesqlCase.getInputs().get(0);
                    }
                    if (null == request || CollectionUtils.isEmpty(request.getColumns())) {
                        log.error("fail to execute in request query sql executor: sql case request columns is empty");
                        reportLog.error("fail to execute in request query sql executor: sql case request columns is empty");
                        return null;
                    }
                    fesqlResult = FesqlUtil.sqlRequestMode(executor, dbName, null == fesqlCase.getBatch_request(), sql, request);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("version:{} execute end",version);
        reportLog.info("version:{} execute end",version);
        return fesqlResult;
    }

    @Override
    protected void prepare(String version,SqlExecutor executor) {
        log.info("version:{} prepare begin",version);
        reportLog.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        reportLog.info("create db:{},{}", dbName, dbOk);
        boolean useFirstInputAsRequests = !isBatchRequest && null == fesqlCase.getBatch_request();
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), useFirstInputAsRequests);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail");
        }
        log.info("version:{} prepare end",version);
        reportLog.info("version:{} prepare end",version);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("request-unsupport")) {
            log.info("skip case in request mode: {}", fesqlCase.getDesc());
            reportLog.info("skip case in request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-unsupport")) {
            log.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            reportLog.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-request-unsupport")) {
            log.info("skip case in rtidb request mode: {}", fesqlCase.getDesc());
            reportLog.info("skip case in rtidb request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (FesqlConfig.isCluster() &&
                null != fesqlCase.getMode() && fesqlCase.getMode().contains("cluster-unsupport")) {
            log.info("cluster-unsupport, skip case in cluster request mode: {}", fesqlCase.getDesc());
            reportLog.info("cluster-unsupport, skip case in cluster request mode: {}", fesqlCase.getDesc());
            return false;
        }
        return true;
    }
}
