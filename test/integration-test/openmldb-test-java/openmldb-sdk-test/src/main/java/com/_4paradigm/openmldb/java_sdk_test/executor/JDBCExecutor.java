package com._4paradigm.openmldb.java_sdk_test.executor;


import com._4paradigm.openmldb.java_sdk_test.checker.Checker;
import com._4paradigm.openmldb.java_sdk_test.checker.CheckerStrategy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/9 2:48 PM
 */
@Slf4j
public abstract class JDBCExecutor extends BaseExecutor{

    public JDBCExecutor(SQLCase fesqlCase, SQLCaseType sqlCaseType) {
        this.fesqlCase = fesqlCase;
        this.executorType = sqlCaseType;
        dbName = fesqlCase.getDb();
        if (!CollectionUtils.isEmpty(fesqlCase.getInputs())) {
            for (InputDesc inputDesc : fesqlCase.getInputs()) {
                tableNames.add(inputDesc.getName());
            }
        }
    }

    @Override
    public void check() throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(fesqlCase, mainResult,executorType);
        for (Checker checker : strategyList) {
            checker.check();
        }
    }
}
