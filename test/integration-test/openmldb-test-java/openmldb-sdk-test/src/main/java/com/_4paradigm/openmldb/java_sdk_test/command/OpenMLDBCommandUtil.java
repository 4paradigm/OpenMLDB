package com._4paradigm.openmldb.java_sdk_test.command;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

@Slf4j
public class OpenMLDBCommandUtil {
    private static final Logger logger = new LogProxy(log);

    public static FesqlResult createDB(FEDBInfo fedbInfo, String dbName) {
        String sql = String.format("create database %s ;",dbName);
        FesqlResult fesqlResult = OpenMLDBComamndFacade.sql(fedbInfo,dbName,sql);
        return fesqlResult;
    }

    public static FesqlResult showTable(FEDBInfo fedbInfo, String dbName, List<InputDesc> inputs) {
        return null;
    }
}
