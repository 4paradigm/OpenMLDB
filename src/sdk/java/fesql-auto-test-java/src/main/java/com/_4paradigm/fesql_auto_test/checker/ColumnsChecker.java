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

package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql.sqlcase.model.Table;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.Schema;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class ColumnsChecker extends BaseChecker {

    public ColumnsChecker(ExpectDesc expect, FesqlResult fesqlResult) {
        super(expect, fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("column name check");
        reportLog.info("column name check");
        List<String> expectColumns = expect.getColumns();
        if (expectColumns == null || expectColumns.size() == 0) {
            return;
        }
        Schema schema = fesqlResult.getResultSchema();
        if (schema != null) {
            Assert.assertEquals(expectColumns.size(), schema.GetColumnCnt(), "Illegal schema size");
            for (int i = 0; i < expectColumns.size(); i++) {
                Assert.assertEquals(schema.GetColumnName(i), Table.getColumnName(expectColumns.get(i)));
                Assert.assertEquals(schema.GetColumnType(i),
                        FesqlUtil.getColumnType(Table.getColumnType(expectColumns.get(i))));
            }
        } else {
            ResultSetMetaData metaData = fesqlResult.getMetaData();
            Assert.assertEquals(expectColumns.size(), metaData.getColumnCount(), "Illegal schema size");
            for (int i = 0; i < expectColumns.size(); i++) {
                Assert.assertEquals(metaData.getColumnName(i + 1), Table.getColumnName(expectColumns.get(i)));
                Assert.assertEquals(metaData.getColumnType(i + 1),
                        FesqlUtil.getSQLType(Table.getColumnType(expectColumns.get(i))));
            }
        }

    }
}
