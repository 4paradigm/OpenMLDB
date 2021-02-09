package com._4paradigm.fesql_auto_test.executor.diff;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.checker.Checker;
import com._4paradigm.fesql_auto_test.checker.CheckerStrategy;
import com._4paradigm.fesql_auto_test.checker.DiffVersionChecker;
import com._4paradigm.fesql_auto_test.entity.FEDBInfo;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.executor.IExecutor;
import com._4paradigm.sql.sdk.SqlExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.ConsoleAppender;
import org.testng.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/15 11:23 AM
 */
@Slf4j
public abstract class BaseDiffExecutor implements IExecutor{
    protected SQLCase fesqlCase;
    protected SqlExecutor executor;
    protected Map<String,SqlExecutor> executorMap;
    protected Map<String,FEDBInfo> fedbInfoMap;

    public BaseDiffExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String,FEDBInfo> fedbInfoMap) {
        this.executorMap = executorMap;
        this.fesqlCase = fesqlCase;
        this.executor = executor;
        this.fedbInfoMap = fedbInfoMap;
    }

    public void run(){
        try {
            boolean verify = verify();
            if(!verify) return;
            String className = Thread.currentThread().getStackTrace()[2].getClassName();
            String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
            log.info(className+"."+methodName+":"+fesqlCase.getDesc() + " Begin!");
            if (null == fesqlCase) {
                Assert.fail("executor run with null case");
                return;
            }
            prepare();
            FesqlResult fesqlResult = execute("mainVersion",executor);
            Map<String, FesqlResult> resultMap = executorMap.entrySet().stream().
                    collect(Collectors.toMap(e -> e.getKey(), e -> execute(e.getKey(),e.getValue())));
            check(fesqlResult,resultMap);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("executor run with exception");
        }finally {
            tearDown();
        }
    }

    protected abstract FesqlResult execute(String version,SqlExecutor executor);

    protected void check(FesqlResult fesqlResult,Map<String,FesqlResult> resultMap) throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, fesqlResult);
        strategyList.add(new DiffVersionChecker(fesqlCase,fesqlResult,resultMap));
        for (Checker checker : strategyList) {
            checker.check();
        }
    }


    public void tearDown() {
    }
}
