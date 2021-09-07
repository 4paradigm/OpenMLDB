package com._4paradigm.openmldb.java_sdk_test.executor;

import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
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
        log.info("skip case in mysql mode: {}", fesqlCase.getDesc());
        reportLog.info("skip case in mysql mode: {}", fesqlCase.getDesc());
        return false;
    }

    @Override
    public void prepare() {
        log.info("mysql prepare begin");
        reportLog.info("mysql prepare begin");
        for(InputDesc inputDesc:fesqlCase.getInputs()) {
            String createSql = MysqlUtil.getCreateTableSql(inputDesc);
            JDBCUtil.executeUpdate(createSql, DBType.MYSQL);
            boolean ok = MysqlUtil.insertData(inputDesc);
            if (!ok) {
                throw new RuntimeException("fail to run MysqlExecutor: prepare fail");
            }
        }
        log.info("mysql prepare end");
        reportLog.info("mysql prepare end");
    }

    @Override
    public void execute() {
        log.info("mysql execute begin");
        reportLog.info("mysql execute begin");
        FesqlResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                sql = FesqlUtil.formatSql(sql, tableNames);
                fesqlResult = JDBCUtil.executeQuery(sql,DBType.MYSQL);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            sql = FesqlUtil.formatSql(sql, tableNames);
            fesqlResult = JDBCUtil.executeQuery(sql,DBType.MYSQL);
        }
        mainResult = fesqlResult;
        log.info("mysql execute end");
        reportLog.info("mysql execute end");
    }

    @Override
    public void tearDown() {
        log.info("mysql,begin drop table");
        reportLog.info("mysql,begin drop table");
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
