package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.UnequalExpect;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.executor.ExecutorFactory;

import java.util.ArrayList;
import java.util.List;

public class CheckerStrategy {

    public static List<Checker> build(SQLCase fesqlCase, FesqlResult fesqlResult, ExecutorFactory.ExecutorType executorType) {

        List<Checker> checkList = new ArrayList<>();
        if (null == fesqlCase) {
            return checkList;
        }
        ExpectDesc expect = getExpectByExecutorType(fesqlCase,executorType);

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

    private static ExpectDesc getExpectByExecutorType(SQLCase fesqlCase, ExecutorFactory.ExecutorType executorType){
        ExpectDesc expect = fesqlCase.getExpect();
        UnequalExpect unequalExpect = fesqlCase.getUnequalExpect();
        if(unequalExpect==null) return expect;
        switch (executorType){
            case kDDL:
            case kBatch:
                ExpectDesc batch_expect = unequalExpect.getBatch_expect();
                if(batch_expect!=null) return batch_expect;
                break;
            case kRequestWithSp:
            case kRequestWithSpAsync:
                ExpectDesc sp_expect = unequalExpect.getSp_expect();
                if(sp_expect!=null) return sp_expect;
            case kRequest:
                ExpectDesc request_expect = unequalExpect.getRequest_expect();
                if(request_expect!=null) return request_expect;
                break;
            case kBatchRequest:
            case kBatchRequestWithSp:
            case kBatchRequestWithSpAsync:
                ExpectDesc request_batch_expect = unequalExpect.getRequest_batch_expect();
                if(request_batch_expect!=null) return request_batch_expect;
                break;
        }
        ExpectDesc realtime_expect = unequalExpect.getRealtime_expect();
        if(realtime_expect!=null) return realtime_expect;
        ExpectDesc euqalsExpect = unequalExpect.getExpect();
        if(euqalsExpect!=null) return euqalsExpect;
        return expect;
    }
}
