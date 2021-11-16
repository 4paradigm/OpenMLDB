/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com._4paradigm.openmldb.java_sdk_test.command;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

@Slf4j
public class OpenMLDBCommandUtil {
    private static final Logger logger = new LogProxy(log);

    public static FesqlResult createDB(FEDBInfo fedbInfo, String dbName) {
        String sql = String.format("create database %s ;",dbName);
        FesqlResult fesqlResult = OpenMLDBComamndFacade.sql(fedbInfo,dbName,sql);
        return fesqlResult;
    }

    public static FesqlResult desc(FEDBInfo fedbInfo, String dbName, String tableName) {
        String sql = String.format("desc %s ;",tableName);
        FesqlResult fesqlResult = OpenMLDBComamndFacade.sql(fedbInfo,dbName,sql);
        return fesqlResult;
    }

    public static FesqlResult createAndInsert(FEDBInfo fedbInfo, String defaultDBName, List<InputDesc> inputs) {
        HashSet<String> dbNames = new HashSet<>();
        if (StringUtils.isNotEmpty(defaultDBName)) {
            dbNames.add(defaultDBName);
        }
        if (!Objects.isNull(inputs)) {
            for (InputDesc input : inputs) {
                // CreateDB if input's db has been configured and hasn't been created before
                if (!StringUtils.isEmpty(input.getDb()) && !dbNames.contains(input.getDb())) {
                    FesqlResult createDBResult = createDB(fedbInfo,input.getDb());
                    dbNames.add(input.getDb());
                    log.info("create db:{},{}", input.getDb(), createDBResult.isOk());
                }
            }
        }
        FesqlResult fesqlResult = new FesqlResult();
        if (inputs != null && inputs.size() > 0) {
            for (int i = 0; i < inputs.size(); i++) {
                InputDesc inputDesc = inputs.get(i);
                String dbName = inputDesc.getDb().isEmpty() ? defaultDBName : inputDesc.getDb();
                String tableName = inputDesc.getName();
                //create table
                String createSql = inputDesc.extractCreate();
                createSql = SQLCase.formatSql(createSql, i, tableName);
                if (!createSql.isEmpty()) {
                    FesqlResult res = OpenMLDBComamndFacade.sql(fedbInfo,dbName,createSql);
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
                        FesqlResult res = OpenMLDBComamndFacade.sql(fedbInfo,dbName,insertSql);
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
}
