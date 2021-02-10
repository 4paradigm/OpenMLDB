package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.checker.Checker;
import com._4paradigm.fesql_auto_test.checker.CheckerStrategy;
import com._4paradigm.fesql_auto_test.checker.DiffVersionChecker;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.collections.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/15 11:23 AM
 */
@Slf4j
public abstract class BaseSQLExecutor implements IExecutor{
    protected SQLCase fesqlCase;
    protected SqlExecutor executor;
    protected Map<String,SqlExecutor> executorMap;
    protected Map<String,FEDBInfo> fedbInfoMap;
    protected ExecutorFactory.ExecutorType executorType;
    protected String dbName;
    protected List<String> tableNames = Lists.newArrayList();

    public BaseSQLExecutor(SqlExecutor executor, SQLCase fesqlCase, ExecutorFactory.ExecutorType executorType) {
        this.executor = executor;
        this.fesqlCase = fesqlCase;
        this.executorType = executorType;
        dbName = fesqlCase.getDb();
        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }
    }

    public BaseSQLExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap, ExecutorFactory.ExecutorType executorType) {
        this(executor,fesqlCase,executorType);
        this.executor = executor;
        this.fedbInfoMap = fedbInfoMap;
    }

    @Override
    public void prepare() throws Exception {
        prepare("mainVersion",executor);
        if(MapUtils.isNotEmpty(executorMap)) {
            executorMap.entrySet().stream().forEach(e -> prepare(e.getKey(), e.getValue()));
        }
    }

    protected abstract void prepare(String mainVersion, SqlExecutor executor);


    public void run(){
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        log.info(className+"."+methodName+":"+fesqlCase.getDesc() + " Begin!");
        try {
            boolean verify = verify();
            if(!verify) return;
            if (null == fesqlCase) {
                Assert.fail("executor run with null case");
                return;
            }
            prepare();
            FesqlResult fesqlResult = execute("mainVersion",executor);
            Map<String, FesqlResult> resultMap = null;
            if(MapUtils.isNotEmpty(executorMap)) {
                resultMap = executorMap.entrySet().stream().
                        collect(Collectors.toMap(e -> e.getKey(), e -> execute(e.getKey(), e.getValue())));
            }
            check(fesqlResult,resultMap);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("executor run with exception");
        }finally {
            tearDown();
        }
    }

    public void check(FesqlResult fesqlResult,Map<String,FesqlResult> resultMap) throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, fesqlResult, executorType);
        if(MapUtils.isNotEmpty(resultMap)) {
            strategyList.add(new DiffVersionChecker(fesqlResult, resultMap));
        }
        for (Checker checker : strategyList) {
            checker.check();
        }
    }
    @Override
    public void tearDown() {
        tearDown("mainVersion",executor);
        if(MapUtils.isNotEmpty(executorMap)) {
            executorMap.entrySet().stream().forEach(e -> tearDown(e.getKey(), e.getValue()));
        }
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
