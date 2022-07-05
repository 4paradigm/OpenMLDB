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
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class BatchSQLExecutor extends BaseSQLExecutor {

    public BatchSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, SQLCaseType executorType) {
        super(executor, fesqlCase, executorType);
    }
    public BatchSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> fedbInfoMap, SQLCaseType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, executorType);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("hybridse-only")) {
            logger.info("skip case in batch mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("batch-unsupport")) {
            logger.info("skip case in batch mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-batch-unsupport")) {
            logger.info("skip case in rtidb batch mode: {}", fesqlCase.getDesc());
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
        return true;
    }

    @Override
    public void prepare(String version,SqlExecutor executor){
        logger.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        logger.info("version:{},create db:{},{}", version, dbName, dbOk);
        OpenMLDBUtil.useDB(executor,dbName);
        OpenMLDBResult res = OpenMLDBUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), false);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail . version:"+version);
        }
        logger.info("version:{} prepare end",version);
    }

    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor){
        logger.info("version:{} execute begin",version);
        OpenMLDBResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                // log.info("sql:{}", sql);
                if(MapUtils.isNotEmpty(fedbInfoMap)) {
                    sql = OpenMLDBUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                }else {
                    sql = OpenMLDBUtil.formatSql(sql, tableNames);
                }
                fesqlResult = OpenMLDBUtil.sql(executor, dbName, sql);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            // log.info("sql:{}", sql);
            if(MapUtils.isNotEmpty(fedbInfoMap)) {
                sql = OpenMLDBUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
            }else {
                sql = OpenMLDBUtil.formatSql(sql, tableNames);
            }
            fesqlResult = OpenMLDBUtil.sql(executor, dbName, sql);
        }
        logger.info("version:{} execute end",version);
        return fesqlResult;
    }
}
