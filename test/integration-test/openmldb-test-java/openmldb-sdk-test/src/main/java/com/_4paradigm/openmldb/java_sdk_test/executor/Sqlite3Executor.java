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
import com._4paradigm.openmldb.java_sdk_test.util.JDBCUtil;
import com._4paradigm.openmldb.java_sdk_test.util.Sqlite3Util;
import com._4paradigm.openmldb.test_common.model.DBType;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/9 3:11 PM
 */
@Slf4j
public class Sqlite3Executor extends JDBCExecutor{
    public Sqlite3Executor(SQLCase fesqlCase, SQLCaseType sqlCaseType) {
        super(fesqlCase,sqlCaseType);
    }

    @Override
    public boolean verify() {
        List<String> sqlDialect = fesqlCase.getSqlDialect();
        if(sqlDialect.contains(DBType.ANSISQL.name())|| sqlDialect.contains(DBType.SQLITE3.name())){
            return true;
        }
        log.info("skip case in sqlite3 mode: {}", fesqlCase.getDesc());
        reportLog.info("skip case in sqlite3 mode: {}", fesqlCase.getDesc());
        return false;
    }

    @Override
    public void prepare() {
        log.info("sqlite3 prepare begin");
        reportLog.info("sqlite3 prepare begin");
        for(InputDesc inputDesc:fesqlCase.getInputs()) {
            String createSql = Sqlite3Util.getCreateTableSql(inputDesc);
            JDBCUtil.executeUpdate(createSql,DBType.SQLITE3);
            boolean ok = Sqlite3Util.insertData(inputDesc);
            if (!ok) {
                throw new RuntimeException("fail to run Sqlite3Executor: prepare fail");
            }
        }
        log.info("sqlite3 prepare end");
        reportLog.info("sqlite3 prepare end");
    }

    @Override
    public void execute() {
        log.info("sqlite3 execute begin");
        reportLog.info("sqlite3 execute begin");
        FesqlResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                sql = FesqlUtil.formatSql(sql, tableNames);
                fesqlResult = JDBCUtil.executeQuery(sql,DBType.SQLITE3);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            sql = FesqlUtil.formatSql(sql, tableNames);
            fesqlResult = JDBCUtil.executeQuery(sql,DBType.SQLITE3);
        }
        mainResult = fesqlResult;
        log.info("sqlite3 execute end");
        reportLog.info("sqlite3 execute end");
    }

    @Override
    public void tearDown() {
        log.info("sqlite3,begin drop table");
        reportLog.info("sqlite3,begin drop table");
        List<InputDesc> tables = fesqlCase.getInputs();
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        for (InputDesc table : tables) {
            if(table.isDrop()) {
                String drop = "drop table " + table.getName();
                JDBCUtil.executeUpdate(drop,DBType.SQLITE3);
            }
        }
    }
}
