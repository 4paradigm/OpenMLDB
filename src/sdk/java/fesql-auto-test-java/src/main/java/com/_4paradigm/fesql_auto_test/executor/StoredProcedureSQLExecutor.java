package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.fesql_auto_test.util.Tool;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

@Slf4j
public class StoredProcedureSQLExecutor extends RequestQuerySQLExecutor{

    public StoredProcedureSQLExecutor(SqlExecutor executor, SQLCase fesqlCase) {
        super(executor, fesqlCase);
    }

    @Override
    protected void prepare() throws Exception {
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), true, 3);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run SQLExecutor: prepare fail");
        }
        for (InputDesc inputDesc : fesqlCase.getInputs()) {
            tableNames.add(inputDesc.getName());
        }
    }

    @Override
    protected FesqlResult execute() throws Exception {
        FesqlResult fesqlResult = null;
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            if (fesqlCase.getInputs().isEmpty() || CollectionUtils.isEmpty(fesqlCase.getInputs().get(0).getRows())) {
                log.error("fail to execute in request query sql executor: sql case inputs is empty");
                return null;
            }
            log.info("sql:{}", sql);
            String spSql = FesqlUtil.getStoredProcedureSql(sql, fesqlCase.getInputs());
            log.info("spSql:{}", spSql);

            sql = FesqlUtil.formatSql(sql, tableNames);
            fesqlResult = FesqlUtil.sqlRequestModeWithSp(executor, dbName, tableNames.get(0), spSql, fesqlCase.getInputs().get(0));
        }
        return fesqlResult;
    }
}
