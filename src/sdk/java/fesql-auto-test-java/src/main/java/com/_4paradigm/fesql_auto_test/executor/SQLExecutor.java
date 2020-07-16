package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.DataType;
import com._4paradigm.sql.ResultSet;
import com._4paradigm.sql.Schema;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class SQLExecutor extends BaseExecutor {

    private String dbName;
    private List<String> tableNames;

    public SQLExecutor(SqlExecutor executor, SQLCase fesqlCase) {
        super(executor, fesqlCase);
        dbName = fesqlCase.getDb();
    }

    @Override
    protected void prepare() throws Exception {
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        tableNames = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs());
    }

    @Override
    protected FesqlResult execute() throws Exception {
        FesqlResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                log.info("sql:{}", sql);
                sql = FesqlUtil.formatSql(sql, tableNames);
                fesqlResult = FesqlUtil.sql(executor, dbName, sql);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            log.info("sql:{}", sql);
            sql = FesqlUtil.formatSql(sql, tableNames);
            fesqlResult = FesqlUtil.sql(executor, dbName, sql);
        }
        return fesqlResult;
    }

}
