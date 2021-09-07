package com._4paradigm.openmldb.java_sdk_test.command;

import com._4paradigm.openmldb.java_sdk_test.command.chain.SqlChainManager;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.List;

@Slf4j
public class OpenMLDBComamndFacade {
    private static final Logger logger = new LogProxy(log);
    public static FesqlResult createAndInsert(FEDBInfo fedbInfo, String dbName, List<InputDesc> inputs) {
        FesqlResult fesqlResult = new FesqlResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                String tableName = inputs.get(i).getName();
                //create table
                String createSql = inputs.get(i).extractCreate();
                createSql = SQLCase.formatSql(createSql, i, tableName);
                if (!createSql.isEmpty()) {
                    FesqlResult res = sql(fedbInfo,dbName,createSql);
                    if (!res.isOk()) {
                        logger.error("fail to create table");
                        // reportLog.error("fail to create table");
                        return res;
                    }
                }
                InputDesc input = inputs.get(i);
                List<String> inserts = input.extractInserts();
                for (String insertSql : inserts) {
                    insertSql = SQLCase.formatSql(insertSql, i, input.getName());
                    if (!insertSql.isEmpty()) {
                        FesqlResult res = sql(fedbInfo,dbName,insertSql);
                        if (!res.isOk()) {
                            logger.error("fail to insert table");
                            // reportLog.error("fail to insert table");
                            return res;
                        }
                    }
                }
            }
        }
        fesqlResult.setOk(true);
        return fesqlResult;
    }
    public static FesqlResult sql(FEDBInfo fedbInfo, String dbName, String sql) {
        logger.info("sql:"+sql);
        FesqlResult fesqlResult = SqlChainManager.of().sql(fedbInfo, dbName, sql);
        logger.info("fesqlResult:"+fesqlResult);
        return fesqlResult;
    }
}
