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

    public RequestQuerySQLExecutor(SqlExecutor executor, SQLCase fesqlCase,
                                   boolean isBatchRequest, boolean isAsyn, SQLCaseType executorType) {
        super(executor, fesqlCase, executorType);
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
        logger.info("version:{} execute begin",version);
        OpenMLDBResult fesqlResult = null;
        try {
             List<String> sqls = fesqlCase.getSqls();
             if (sqls != null && sqls.size() > 0) {
                 for (String sql : sqls) {
                     // log.info("sql:{}", sql);
                     if(MapUtils.isNotEmpty(fedbInfoMap)) {
                         sql = SQLUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                     }else {
                         sql = SQLUtil.formatSql(sql, tableNames);
                     }
                     fesqlResult = SDKUtil.sql(executor, dbName, sql);
                 }
             }
            String sql = fesqlCase.getSql();
            if (sql != null && sql.length() > 0) {
                // log.info("sql:{}", sql);
                if(MapUtils.isNotEmpty(fedbInfoMap)) {
                    sql = SQLUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                }else {
                    sql = SQLUtil.formatSql(sql, tableNames);
                }
                InputDesc request = null;
                if (isBatchRequest) {
                    InputDesc batchRequest = fesqlCase.getBatch_request();
                    if (batchRequest == null) {
                        logger.error("No batch request provided in case");
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
                    if (null != fesqlCase.getBatch_request()) {
                        request = fesqlCase.getBatch_request();
                    } else if (!fesqlCase.getInputs().isEmpty()) {
                        request = fesqlCase.getInputs().get(0);
                    }
                    if (null == request || CollectionUtils.isEmpty(request.getColumns())) {
                        logger.error("fail to execute in request query sql executor: sql case request columns is empty");
                        return null;
                    }
                    fesqlResult = SDKUtil.sqlRequestMode(executor, dbName, null == fesqlCase.getBatch_request(), sql, request);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        logger.info("version:{} execute end",version);
        return fesqlResult;
    }

    @Override
    protected void prepare(String version,SqlExecutor executor) {
        logger.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        logger.info("create db:{},{}", dbName, dbOk);
        boolean useFirstInputAsRequests = !isBatchRequest && null == fesqlCase.getBatch_request();
        OpenMLDBResult res = SDKUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), useFirstInputAsRequests);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail");
        }
        logger.info("version:{} prepare end",version);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("hybridse-only")) {
            logger.info("skip case in request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("request-unsupport")) {
            logger.info("skip case in request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-unsupport")) {
            logger.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("performance-sensitive-unsupport")) {
            logger.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-request-unsupport")) {
            logger.info("skip case in rtidb request mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (OpenMLDBConfig.isCluster() &&
                null != fesqlCase.getMode() && fesqlCase.getMode().contains("cluster-unsupport")) {
            logger.info("cluster-unsupport, skip case in cluster request mode: {}", fesqlCase.getDesc());
            return false;
        }
        return true;
    }
}
