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
package com._4paradigm.openmldb.test_common.command;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.command.chain.SqlChainManager;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.List;

@Slf4j
public class OpenMLDBCommandFacade {
//    private static final Logger logger = new LogProxy(log);
    public static OpenMLDBResult sql(OpenMLDBInfo openMLDBInfo, String dbName, String sql) {
        log.info("sql:"+sql);
        sql = StringUtils.replace(sql,"\n"," ");
        sql = sql.trim();
        OpenMLDBResult openMLDBResult = SqlChainManager.of().sql(openMLDBInfo, dbName, sql);
        log.info("openMLDBResult:"+openMLDBResult);
        return openMLDBResult;
    }
    public static OpenMLDBResult sqls(OpenMLDBInfo openMLDBInfo, String dbName, List<String> sqls) {
        OpenMLDBResult openMLDBResult = null;
        for(String sql:sqls){
            openMLDBResult = sql(openMLDBInfo,dbName,sql);
        }
        return openMLDBResult;
    }
}
