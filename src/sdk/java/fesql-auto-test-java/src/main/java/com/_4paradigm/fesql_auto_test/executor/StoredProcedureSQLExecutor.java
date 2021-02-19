package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class StoredProcedureSQLExecutor extends RequestQuerySQLExecutor {

    private List<String> spNames;

    public StoredProcedureSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, boolean isBatchRequest, boolean isAsyn, ExecutorFactory.ExecutorType executorType) {
        super(executor, fesqlCase, isBatchRequest, isAsyn, executorType);
        spNames = new ArrayList<>();
    }

    public StoredProcedureSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap, boolean isBatchRequest, boolean isAsyn, ExecutorFactory.ExecutorType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, isBatchRequest, isAsyn, executorType);
        spNames = new ArrayList<>();
    }

    @Override
    public void prepare(String version,SqlExecutor executor){
        log.info("version:{} prepare begin",version);
        reportLog.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        log.info("create db:{},{}", dbName, dbOk);
        reportLog.info("create db:{},{}", dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(
                executor, dbName, fesqlCase.getInputs(),
                !isBatchRequest && null == fesqlCase.getBatch_request(), 1);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail");
        }
        log.info("version:{} prepare end",version);
        reportLog.info("version:{} prepare end",version);
    }
    @Override
    public FesqlResult execute(String version,SqlExecutor executor) {
        log.info("version:{} execute begin",version);
        reportLog.info("version:{} execute begin",version);
        FesqlResult fesqlResult = null;
        try {
            if (fesqlCase.getInputs().isEmpty() ||
                    CollectionUtils.isEmpty(fesqlCase.getInputs().get(0).getRows())) {
                log.error("fail to execute in request query sql executor: sql case inputs is empty");
                reportLog.error("fail to execute in request query sql executor: sql case inputs is empty");
                return null;
            }
            String sql = fesqlCase.getSql();
            log.info("sql: {}", sql);
            reportLog.info("sql: {}", sql);
            if (sql == null || sql.length() == 0) {
                return null;
            }
            if (fesqlCase.getBatch_request() != null) {
                fesqlResult = executeBatch(executor, sql, this.isAsyn);
            } else {
                fesqlResult = executeSingle(executor, sql, this.isAsyn);
            }
            spNames.add(fesqlCase.getSpName());
        }catch (Exception e){
            e.printStackTrace();
        }
        log.info("version:{} execute end",version);
        reportLog.info("version:{} execute end",version);
        return fesqlResult;
    }

    private FesqlResult executeSingle(SqlExecutor executor, String sql, boolean isAsyn) throws SQLException {
        String spSql = fesqlCase.getProcedure(sql);
        log.info("spSql: {}", spSql);
        reportLog.info("spSql: {}", spSql);
        return FesqlUtil.sqlRequestModeWithSp(
                executor, dbName, fesqlCase.getSpName(), null == fesqlCase.getBatch_request(),
                spSql, fesqlCase.getInputs().get(0), isAsyn);
    }

    private FesqlResult executeBatch(SqlExecutor executor, String sql, boolean isAsyn) throws SQLException {
        String spName = "sp_" + tableNames.get(0) + "_" + System.currentTimeMillis();
        String spSql = FesqlUtil.buildSpSQLWithConstColumns(spName, sql, fesqlCase.getBatch_request());
        log.info("spSql: {}", spSql);
        reportLog.info("spSql: {}", spSql);
        return FesqlUtil.selectBatchRequestModeWithSp(
                executor, dbName, spName, spSql, fesqlCase.getBatch_request(), isAsyn);
    }


    @Override
    public void tearDown(String version,SqlExecutor executor) {
        log.info("version:{},begin drop table",version);
        reportLog.info("version:{},begin drop table",version);
        if (CollectionUtils.isEmpty(spNames)) {
            return;
        }
        for (String spName : spNames) {
            String drop = "drop procedure " + spName + ";";
            FesqlUtil.ddl(executor, dbName, drop);
        }
        super.tearDown(version,executor);
    }
}
