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
import com._4paradigm.openmldb.java_sdk_test.util.JDBCUtil;
import com._4paradigm.openmldb.java_sdk_test.util.MysqlUtil;
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
public class MysqlExecutor extends JDBCExecutor{
    public MysqlExecutor(SQLCase fesqlCase, SQLCaseType sqlCaseType) {
        super(fesqlCase,sqlCaseType);
    }

    @Override
    public boolean verify() {
        List<String> sqlDialect = fesqlCase.getSqlDialect();
        if(sqlDialect.contains(DBType.ANSISQL.name())|| sqlDialect.contains(DBType.MYSQL.name())){
            return true;
        }
        logger.info("skip case in mysql mode: {}", fesqlCase.getDesc());
        return false;
    }

    @Override
    public void prepare() {
        logger.info("mysql prepare begin");
        for(InputDesc inputDesc:fesqlCase.getInputs()) {
            String createSql = MysqlUtil.getCreateTableSql(inputDesc);
            JDBCUtil.executeUpdate(createSql, DBType.MYSQL);
            boolean ok = MysqlUtil.insertData(inputDesc);
            if (!ok) {
                throw new RuntimeException("fail to run MysqlExecutor: prepare fail");
            }
        }
        logger.info("mysql prepare end");
    }

    @Override
    public void execute() {
        logger.info("mysql execute begin");
        OpenMLDBResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                sql = OpenMLDBUtil.formatSql(sql, tableNames);
                fesqlResult = JDBCUtil.executeQuery(sql,DBType.MYSQL);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            sql = OpenMLDBUtil.formatSql(sql, tableNames);
            fesqlResult = JDBCUtil.executeQuery(sql,DBType.MYSQL);
        }
        mainResult = fesqlResult;
        logger.info("mysql execute end");
    }

    @Override
    public void tearDown() {
        logger.info("mysql,begin drop table");
        List<InputDesc> tables = fesqlCase.getInputs();
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        for (InputDesc table : tables) {
            if(table.isDrop()) {
                String drop = "drop table " + table.getName();
                JDBCUtil.executeUpdate(drop,DBType.MYSQL);
            }
        }
    }
}
