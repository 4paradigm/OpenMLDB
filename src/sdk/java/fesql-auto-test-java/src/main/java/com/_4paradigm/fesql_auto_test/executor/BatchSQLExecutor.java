package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class BatchSQLExecutor extends BaseSQLExecutor {

    public BatchSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, ExecutorFactory.ExecutorType executorType) {
        super(executor, fesqlCase, executorType);
    }
    public BatchSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap, ExecutorFactory.ExecutorType executorType) {
        super(fesqlCase, executor, executorMap, fedbInfoMap, executorType);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("batch-unsupport")) {
            log.info("skip case in batch mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-batch-unsupport")) {
            log.info("skip case in rtidb batch mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("rtidb-unsupport")) {
            log.info("skip case in rtidb mode: {}", fesqlCase.getDesc());
            return false;
        }
        return true;
    }

    @Override
    public void prepare(String version,SqlExecutor executor){
        log.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        log.info("version:{},create db:{},{}", version, dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), false);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail . version:"+version);
        }
        log.info("version:{} prepare end",version);
    }

    @Override
    public FesqlResult execute(String version,SqlExecutor executor){
        log.info("version:{} execute begin",version);
        FesqlResult fesqlResult = null;
        List<String> sqls = fesqlCase.getSqls();
        if (sqls != null && sqls.size() > 0) {
            for (String sql : sqls) {
                // log.info("sql:{}", sql);
                sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
                fesqlResult = FesqlUtil.sql(executor, dbName, sql);
            }
        }
        String sql = fesqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            // log.info("sql:{}", sql);
            sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
            fesqlResult = FesqlUtil.sql(executor, dbName, sql);
        }
        log.info("version:{} execute end",version);
        return fesqlResult;
    }
}
