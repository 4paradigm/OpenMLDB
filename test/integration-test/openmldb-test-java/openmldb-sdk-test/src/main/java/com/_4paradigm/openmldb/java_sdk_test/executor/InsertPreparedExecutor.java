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


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class InsertPreparedExecutor extends BatchSQLExecutor {
    private static final Logger logger = new LogProxy(log);
    public InsertPreparedExecutor(SqlExecutor executor, SQLCase fesqlCase, SQLCaseType executorType) {
        super(executor, fesqlCase, executorType);
    }
    public InsertPreparedExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, FEDBInfo> fedbInfoMap, SQLCaseType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, executorType);
    }

    @Override
    public void prepare(String version,SqlExecutor executor){
        logger.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        logger.info("version:{},create db:{},{}", version, dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsertWithPrepared(executor, dbName, fesqlCase.getInputs(), false);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail . version:"+version);
        }
        logger.info("version:{} prepare end",version);
    }
}
