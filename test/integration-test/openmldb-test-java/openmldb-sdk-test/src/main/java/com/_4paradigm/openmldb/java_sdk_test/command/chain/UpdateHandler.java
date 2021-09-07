package com._4paradigm.openmldb.java_sdk_test.command.chain;

import com._4paradigm.openmldb.java_sdk_test.command.OpenmlDBCommandFactory;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

public class UpdateHandler extends AbstractSQLHandler{
    @Override
    public boolean preHandle(String sql) {
        return !(sql.split("\\s+")[0].equalsIgnoreCase("select"));
    }

    @Override
    public FesqlResult onHandle(FEDBInfo fedbInfo, String dbName, String sql) {
        FesqlResult fesqlResult = new FesqlResult();
        List<String> result = OpenmlDBCommandFactory.runNoInteractive(fedbInfo,dbName,sql);
        fesqlResult.setMsg(result.get(0));
        fesqlResult.setOk(success(result));
        return fesqlResult;
    }
    private boolean success(List<String> result){
        if(CollectionUtils.isNotEmpty(result)){
            long failed = result.stream().filter(s -> s.contains("failed")||s.contains("fail")).count();
            return failed==0;
        }
        return true;
    }
}
