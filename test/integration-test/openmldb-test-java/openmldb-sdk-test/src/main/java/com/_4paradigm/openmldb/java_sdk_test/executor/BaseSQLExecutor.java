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
import com._4paradigm.openmldb.java_sdk_test.checker.DiffVersionChecker;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/15 11:23 AM
 */
@Slf4j
public abstract class BaseSQLExecutor extends BaseExecutor{
    protected SqlExecutor executor;
    private Map<String,SqlExecutor> executorMap;
    protected Map<String, FEDBInfo> fedbInfoMap;
    private Map<String, FesqlResult> resultMap;

    public BaseSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, SQLCaseType executorType) {
        this.executor = executor;
        this.fesqlCase = fesqlCase;
        this.executorType = executorType;
        dbName = fesqlCase.getDb();
        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }
    }

    public BaseSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, FEDBInfo> fedbInfoMap, SQLCaseType executorType) {
        this(executor,fesqlCase,executorType);
        this.executor = executor;
        this.executorMap = executorMap;
        this.fedbInfoMap = fedbInfoMap;
    }

    @Override
    public void prepare(){
        prepare("mainVersion",executor);
        if(MapUtils.isNotEmpty(executorMap)) {
            executorMap.entrySet().stream().forEach(e -> prepare(e.getKey(), e.getValue()));
        }
    }

    protected abstract void prepare(String mainVersion, SqlExecutor executor);

    @Override
    public void execute() {
        mainResult = execute("mainVersion",executor);
        mainResult.setDbName(dbName);
        if(CollectionUtils.isNotEmpty(tableNames)) {
            mainResult.setTableNames(tableNames);
        }
        if(MapUtils.isNotEmpty(executorMap)) {
            resultMap = executorMap.entrySet().stream().
                    collect(Collectors.toMap(e -> e.getKey(), e -> execute(e.getKey(), e.getValue())));
        }
    }

    protected abstract FesqlResult execute(String version, SqlExecutor executor);

    @Override
    public void check() throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, mainResult, executorType);
        if(MapUtils.isNotEmpty(resultMap)) {
            strategyList.add(new DiffVersionChecker(mainResult, resultMap));
        }
        for (Checker checker : strategyList) {
            checker.check();
        }
    }
    @Override
    public void tearDown() {
        tearDown("mainVersion",executor);
        if(MapUtils.isNotEmpty(executorMap)) {
            executorMap.entrySet().stream().forEach(e -> tearDown(e.getKey(), e.getValue()));
        }
    }


    public void tearDown(String version,SqlExecutor executor) {
        log.info("version:{},begin drop table",version);
        reportLog.info("version:{},begin drop table",version);
        List<InputDesc> tables = fesqlCase.getInputs();
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        for (InputDesc table : tables) {
            if(table.isDrop()) {
                String drop = "drop table " + table.getName() + ";";
                FesqlUtil.ddl(executor, dbName, drop);
            }
        }
    }
}
