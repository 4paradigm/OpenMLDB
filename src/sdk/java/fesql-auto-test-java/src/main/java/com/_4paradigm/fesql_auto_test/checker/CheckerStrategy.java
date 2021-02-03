package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;

import java.util.ArrayList;
import java.util.List;

public class CheckerStrategy {

    public static List<Checker> build(SQLCase fesqlCase, FesqlResult fesqlResult) {
        List<Checker> checkList = new ArrayList<>();
        if (null == fesqlCase) {
            return checkList;
        }
        ExpectDesc expect = fesqlCase.getExpect();

        // if (false == expect.getSuccess()) {
            checkList.add(new SuccessChecker(fesqlCase, fesqlResult));
        // }
        if (!expect.getColumns().isEmpty()) {
            checkList.add(new ColumnsChecker(fesqlCase, fesqlResult));
        }
        if (!expect.getRows().isEmpty()) {
            checkList.add(new ResultChecker(fesqlCase, fesqlResult));
        }

        if (expect.getCount() >= 0) {
            checkList.add(new CountChecker(fesqlCase, fesqlResult));
        }
        return checkList;
    }
}
