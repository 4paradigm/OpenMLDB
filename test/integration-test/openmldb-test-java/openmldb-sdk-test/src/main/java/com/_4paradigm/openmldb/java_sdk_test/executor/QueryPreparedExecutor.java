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

package com._4paradigm.openmldb.java_sdk_test.executor;

import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zhaowei
 * @date 2020/6/15 11:29 AM
 */
@Slf4j
public class QueryPreparedExecutor extends BatchSQLExecutor {

    public QueryPreparedExecutor(SqlExecutor executor, SQLCase sqlCase, SQLCaseType executorType) {
        super(executor, sqlCase, executorType);
    }
    public QueryPreparedExecutor(SQLCase fesqlCase, SqlExecutor executor, Map<String,SqlExecutor> executorMap, Map<String, OpenMLDBInfo> openMLDBInfoMap, SQLCaseType executorType) {
        super(fesqlCase, executor, executorMap, openMLDBInfoMap, executorType);
    }

    @Override
    public OpenMLDBResult execute(String version, SqlExecutor executor){
        log.info("version:{} execute begin",version);
        OpenMLDBResult fesqlResult = null;
        // List<String> sqls = fesqlCase.getSqls();
        // if (sqls != null && sqls.size() > 0) {
        //     for (String sql : sqls) {
        //         // log.info("sql:{}", sql);
        //         if(MapUtils.isNotEmpty(fedbInfoMap)) {
        //             sql = FesqlUtil.formatSql(sql, tableNames, fedbInfoMap.get(version));
        //         }else {
        //             sql = FesqlUtil.formatSql(sql, tableNames);
        //         }
        //         fesqlResult = FesqlUtil.sql(executor, dbName, sql);
        //     }
        // }
        String sql = sqlCase.getSql();
        if (sql != null && sql.length() > 0) {
            // log.info("sql:{}", sql);
            if(MapUtils.isNotEmpty(openMLDBInfoMap)) {
                sql = SQLUtil.formatSql(sql, tableNames, openMLDBInfoMap.get(version));
            }else {
                sql = SQLUtil.formatSql(sql, tableNames);
            }
            InputDesc parameters = sqlCase.getParameters();
            List<String> types = parameters.getColumns().stream().map(s -> s.split("\\s+")[1]).collect(Collectors.toList());
            List<Object> objects = parameters.getRows().get(0);
            fesqlResult = SDKUtil.selectWithPrepareStatement(executor, dbName,sql, types,objects);
        }
        log.info("version:{} execute end",version);
        return fesqlResult;
    }
}
