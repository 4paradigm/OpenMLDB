package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

@Slf4j
public class RequestQuerySQLExecutor extends SQLExecutor {

    public RequestQuerySQLExecutor(SqlExecutor executor, SQLCase fesqlCase) {
        super(executor, fesqlCase);
    }

    @Override
    protected void prepare() throws Exception {
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), true);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run SQLExecutor: prepare fail");
        }
        for (InputDesc inputDesc : fesqlCase.getInputs()) {
            tableNames.add(inputDesc.getName());
        }
    }

    @Override
    public void run() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("request-unsupport")) {
            log.info("skip case in batch mode: {}", fesqlCase.getDesc());
            return;
        }
        process();
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
            if (fesqlCase.getInputs().isEmpty() || CollectionUtils.isEmpty(fesqlCase.getInputs().get(0).getRows())) {
                log.error("fail to execute in request query sql executor: sql case inputs is empty");
                return null;
            }
            log.info("sql:{}", sql);
            sql = FesqlUtil.formatSql(sql, tableNames);
            fesqlResult = FesqlUtil.sqlRequestMode(executor, dbName, sql, fesqlCase.getInputs().get(0));
        }
        return fesqlResult;
    }
}
