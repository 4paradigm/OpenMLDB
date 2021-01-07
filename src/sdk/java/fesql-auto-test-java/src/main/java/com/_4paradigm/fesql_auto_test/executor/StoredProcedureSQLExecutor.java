package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.fesql_auto_test.util.Tool;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class StoredProcedureSQLExecutor extends RequestQuerySQLExecutor{

    private List<String> spNames;

    public StoredProcedureSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, boolean isBatchRequest, boolean isAsyn) {
        super(executor, fesqlCase, isBatchRequest, isAsyn);
    }

    @Override
    protected void prepare() throws Exception {
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(
                executor, dbName, fesqlCase.getInputs(), !isBatchRequest, 1);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run SQLExecutor: prepare fail");
        }
        for (InputDesc inputDesc : fesqlCase.getInputs()) {
            tables = fesqlCase.getInputs();
            tableNames.add(inputDesc.getName());
        }
        spNames = new ArrayList<>();
    }

    @Override
    protected FesqlResult execute() throws Exception {
        if (fesqlCase.getInputs().isEmpty() ||
            CollectionUtils.isEmpty(fesqlCase.getInputs().get(0).getRows())) {
            log.error("fail to execute in request query sql executor: sql case inputs is empty");
            return null;
        }
        String sql = fesqlCase.getSql();
        log.info("sql: {}", sql);
        if (sql == null || sql.length() == 0) {
            return null;
        }
        FesqlResult fesqlResult;
        if (fesqlCase.getBatch_request() != null) {
            fesqlResult = executeBatch(sql, this.isAsyn);
        } else {
            fesqlResult =  executeSingle(sql, this.isAsyn);
        }
        spNames.add(fesqlCase.getSpName());
        return fesqlResult;
    }

    private FesqlResult executeSingle(String sql, boolean isAsyn) throws SQLException {
        String spSql = fesqlCase.getProcedure(sql);
        log.info("spSql: {}", spSql);
        return FesqlUtil.sqlRequestModeWithSp(
                executor, dbName, fesqlCase.getSpName(), spSql, fesqlCase.getInputs().get(0), isAsyn);
    }

    private FesqlResult executeBatch(String sql, boolean isAsyn) throws SQLException {
        String spName = "sp_" + tableNames.get(0) + "_" + System.currentTimeMillis();
        String spSql = buildSpSQLWithConstColumns(spName, sql, fesqlCase.getBatch_request());
        log.info("spSql: {}", spSql);
        return FesqlUtil.selectBatchRequestModeWithSp(
                executor, dbName, spName, spSql, fesqlCase.getBatch_request(), isAsyn);
    }

    private String buildSpSQLWithConstColumns(String spName,
                                              String sql,
                                              InputDesc input) throws SQLException {
        StringBuilder builder = new StringBuilder("create procedure " + spName + "(\n");
        HashSet<Integer> commonColumnIndices = new HashSet<>();
        if (input.getCommon_column_indices() != null) {
            for (String str : input.getCommon_column_indices()) {
                if (str != null) {
                    commonColumnIndices.add(Integer.parseInt(str));
                }
            }
        }
        if (input.getColumns() == null) {
            throw new SQLException("No schema defined in input desc");
        }
        for(int i = 0; i < input.getColumns().size(); ++i) {
            String[] parts = input.getColumns().get(i).split(" ");
            if (commonColumnIndices.contains(i)) {
                builder.append("const ");
            }
            builder.append(parts[0]);
            builder.append(" ");
            builder.append(parts[1]);
            if (i != input.getColumns().size() - 1) {
                builder.append(",");
            }
        }
        builder.append(")\n");
        builder.append("BEGIN\n");
        builder.append(sql);
        builder.append("\n");
        builder.append("END;");
        sql = builder.toString();
        return sql;
    }

    @Override
    protected void tearDown() {
        super.tearDown();
        if (CollectionUtils.isEmpty(spNames)) {
            return;
        }
        for (String spName : spNames) {
            String drop = "drop procedure " + spName + ";";
            FesqlUtil.ddl(executor, dbName, drop);
        }
    }
}
