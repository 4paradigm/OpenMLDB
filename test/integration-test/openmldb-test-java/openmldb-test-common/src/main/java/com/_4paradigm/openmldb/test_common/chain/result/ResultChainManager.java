package com._4paradigm.openmldb.test_common.chain.result;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;

import java.sql.Statement;

public class ResultChainManager {
    private AbstractResultHandler resultHandler;
    private ResultChainManager() {
        ResultSetHandler selectResultHandler = new ResultSetHandler();

        resultHandler = selectResultHandler;
    }

    private static class ClassHolder {
        private static final ResultChainManager holder = new ResultChainManager();
    }

    public static ResultChainManager of() {
        return ClassHolder.holder;
    }
    public void toOpenMLDBResult(Statement statement, OpenMLDBResult openMLDBResult){
        resultHandler.doHandle(statement,openMLDBResult);
    }
    
}
