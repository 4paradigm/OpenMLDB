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

import com._4paradigm.openmldb.java_sdk_test.command.chain.SqlChainManager;
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
public class OpenMLDBComamndFacade {
    private static final Logger logger = new LogProxy(log);
    public static FesqlResult sql(FEDBInfo fedbInfo, String dbName, String sql) {
        logger.info("sql:"+sql);
        sql = StringUtils.replace(sql,"\n"," ");
        FesqlResult fesqlResult = SqlChainManager.of().sql(fedbInfo, dbName, sql);
        logger.info("fesqlResult:"+fesqlResult);
        return fesqlResult;
    }
    public static FesqlResult sqls(FEDBInfo fedbInfo, String dbName, List<String> sqls) {
        FesqlResult fesqlResult = null;
        for(String sql:sqls){
            fesqlResult = sql(fedbInfo,dbName,sql);
        }
        return fesqlResult;
    }
}
