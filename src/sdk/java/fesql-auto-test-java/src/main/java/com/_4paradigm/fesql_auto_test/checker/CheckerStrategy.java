package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.SQLCaseType;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;

import java.util.ArrayList;
import java.util.List;

public class CheckerStrategy {

    public static List<Checker> build(SQLCase fesqlCase, FesqlResult fesqlResult, SQLCaseType executorType) {

        List<Checker> checkList = new ArrayList<>();
        if (null == fesqlCase) {
            return checkList;
        }
        ExpectDesc expect = fesqlCase.getOnlineExpectByType(executorType);

        checkList.add(new SuccessChecker(expect, fesqlResult));

        if (!expect.getColumns().isEmpty()) {
            checkList.add(new ColumnsChecker(expect, fesqlResult));
        }
        if (!expect.getRows().isEmpty()) {
            checkList.add(new ResultChecker(expect, fesqlResult));
        }

        if (expect.getCount() >= 0) {
            checkList.add(new CountChecker(expect, fesqlResult));
        }
        return checkList;
    }

}
