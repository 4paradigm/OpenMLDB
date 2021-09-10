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
import com._4paradigm.openmldb.java_sdk_test.checker.DiffResultChecker;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.model.DBType;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2021/3/10 8:20 PM
 */
@Slf4j
public class DiffResultExecutor extends BatchSQLExecutor{
    private List<BaseExecutor> executors;
    private Map<String, FesqlResult> resultMap;
    public DiffResultExecutor(SqlExecutor executor, SQLCase fesqlCase, SQLCaseType executorType) {
        super(executor, fesqlCase, executorType);
        executors = new ArrayList<>();
        resultMap = new HashMap<>();
        List<String> sqlDialect = fesqlCase.getSqlDialect();
        if(CollectionUtils.isNotEmpty(sqlDialect)){
            for(String dbType:sqlDialect){
                if(dbType.equals(DBType.SQLITE3.name())||dbType.equals(DBType.ANSISQL.name())){
                    executors.add(new Sqlite3Executor(fesqlCase,SQLCaseType.kSQLITE3));
                }else if(dbType.equals(DBType.MYSQL.name())||dbType.equals(DBType.ANSISQL.name())){
                    executors.add(new MysqlExecutor(fesqlCase,SQLCaseType.kMYSQL));
                }
            }
        }
    }

    @Override
    public boolean verify() {
        boolean verify = super.verify();
        for(IExecutor e:executors){
            boolean eVerify = e.verify();
            verify = eVerify&&verify;
        }
        return verify;
    }

    @Override
    public void prepare() {
        super.prepare("mainVersion",executor);
        executors.stream().forEach(e->e.prepare());
    }

    @Override
    public void execute() {
        mainResult = execute("mainVersion",executor);
        executors.stream().forEach(e->{
            e.execute();
            resultMap.put(e.executorType.getTypeName(),e.mainResult);
        });
    }

    @Override
    public void tearDown() {
        super.tearDown("mainVersion",executor);
        executors.stream().forEach(e->e.tearDown());
    }

    @Override
    public void check() throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, mainResult, executorType);
        strategyList.add(new DiffResultChecker(mainResult, resultMap));
        for (Checker checker : strategyList) {
            checker.check();
        }
    }
}
