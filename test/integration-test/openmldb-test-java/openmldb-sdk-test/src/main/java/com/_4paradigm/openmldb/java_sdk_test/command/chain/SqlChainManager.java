package com._4paradigm.openmldb.java_sdk_test.command.chain;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;

public class SqlChainManager {
    private AbstractSQLHandler sqlHandler;
    private SqlChainManager() {
        sqlHandler = initHandler();
    }

    private AbstractSQLHandler initHandler(){
        QueryHandler queryHandler = new QueryHandler();
        UpdateHandler updateHandler = new UpdateHandler();
        queryHandler.setNextHandler(updateHandler);
        return queryHandler;
    }

    private static class ClassHolder {
        private static final SqlChainManager holder = new SqlChainManager();
    }

    public static SqlChainManager of() {
        return ClassHolder.holder;
    }
    public FesqlResult sql(FEDBInfo fedbInfo, String dbName, String sql){
        FesqlResult fesqlResult = sqlHandler.doHandle(fedbInfo, dbName, sql);
        return fesqlResult;
    }
}
