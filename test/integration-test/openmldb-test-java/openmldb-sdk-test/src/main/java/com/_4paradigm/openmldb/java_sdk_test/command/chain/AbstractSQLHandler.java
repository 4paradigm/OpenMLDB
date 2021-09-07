package com._4paradigm.openmldb.java_sdk_test.command.chain;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import lombok.Setter;

@Setter
public abstract class AbstractSQLHandler {
    private AbstractSQLHandler nextHandler;

    public abstract boolean preHandle(String sql);

    public abstract FesqlResult onHandle(FEDBInfo fedbInfo, String dbName, String sql);

    public FesqlResult doHandle(FEDBInfo fedbInfo, String dbName,String sql){
        if(preHandle(sql)){
            return onHandle(fedbInfo,dbName,sql);
        }
        if(nextHandler!=null){
            return nextHandler.doHandle(fedbInfo,dbName,sql);
        }
        throw new RuntimeException("no next chain");
    }
}
