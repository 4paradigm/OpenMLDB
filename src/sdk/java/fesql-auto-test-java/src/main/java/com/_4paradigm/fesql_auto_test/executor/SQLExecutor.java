package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.testng.collections.Lists;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class SQLExecutor extends BaseExecutor {

    protected String dbName;
    protected List<String> tableNames = Lists.newArrayList();
    protected List<InputDesc> tables;

    public SQLExecutor(SqlExecutor executor, SQLCase fesqlCase) {
        super(executor, fesqlCase);
        dbName = fesqlCase.getDb();
    }

    @Override
    public void run() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("batch-unsupport")) {
            log.info("skip case in batch mode: {}", fesqlCase.getDesc());
            return;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-batch-unsupport")) {
            log.info("skip case in rtidb batch mode: {}", fesqlCase.getDesc());
            return;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-unsupport")) {
            log.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return;
        }
        process();
    }

    @Override
    public void prepare() throws Exception {
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), false);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run SQLExecutor: prepare fail");
        }

        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            tables = fesqlCase.getInputs();
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }

    }

    @Override
    public FesqlResult execute() throws Exception {
        FesqlResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                // log.info("sql:{}", sql);
                sql = FesqlUtil.formatSql(sql, tableNames);
                fesqlResult = FesqlUtil.sql(executor, dbName, sql);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
           // log.info("sql:{}", sql);
           sql = FesqlUtil.formatSql(sql, tableNames);
            fesqlResult = FesqlUtil.sql(executor, dbName, sql);
        }
        return fesqlResult;
    }

    @Override
    public void tearDown() {
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
