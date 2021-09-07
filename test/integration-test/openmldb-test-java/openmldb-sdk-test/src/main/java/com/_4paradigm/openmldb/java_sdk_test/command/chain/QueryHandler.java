package com._4paradigm.openmldb.java_sdk_test.command.chain;



import com._4paradigm.openmldb.java_sdk_test.command.OpenmlDBCommandFactory;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;

import java.util.List;

public class QueryHandler extends AbstractSQLHandler{
    @Override
    public boolean preHandle(String sql) {
        return sql.split("\\s+")[0].equalsIgnoreCase("select");
    }

    @Override
    public FesqlResult onHandle(FEDBInfo fedbInfo, String dbName, String sql) {
        FesqlResult fesqlResult = new FesqlResult();
        List<String> result = OpenmlDBCommandFactory.runNoInteractive(fedbInfo,dbName,sql);
        System.out.println("select result = " + result);
        fesqlResult.setMsg(result.get(0));
        fesqlResult.setOk(true);
        return fesqlResult;
    }
}
