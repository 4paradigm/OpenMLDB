package com._4paradigm.fesql_auto_test.executor.diff;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.executor.BaseExecutor;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.testng.collections.Lists;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class DiffVersionSQLExecutor extends BaseDiffExecutor {

    protected String dbName;
    protected List<String> tableNames = Lists.newArrayList();

    public DiffVersionSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap) {
        super(fesqlCase, executor, executorMap, fedbInfoMap);
        dbName = fesqlCase.getDb();
        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }
    }

    @Override
    protected FesqlResult execute(String version,SqlExecutor executor){
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
    public void prepare() throws Exception {
        prepare("mainVersion",executor);
        executorMap.entrySet().stream().forEach(e->prepare(e.getKey(),e.getValue()));
    }
    public void prepare(String version,SqlExecutor executor){
        log.info("version:{} prepare begin",version);
        boolean dbOk = executor.createDB(dbName);
        log.info("version:{},create db:{},{}", version, dbName, dbOk);
        FesqlResult res = FesqlUtil.createAndInsert(executor, dbName, fesqlCase.getInputs(), false);
        if (!res.isOk()) {
            throw new RuntimeException("fail to run SQLExecutor: prepare fail . version:"+version);
        }
        log.info("version:{} prepare end",version);
    }

    @Override
    public void tearDown() {
        tearDown("mainVersion",executor);
        executorMap.entrySet().stream().forEach(e->tearDown(e.getKey(),e.getValue()));
    }

    public void tearDown(String version,SqlExecutor executor) {
        log.info("version:{},begin drop table",version);
        List<InputDesc> tables = fesqlCase.getInputs();
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
