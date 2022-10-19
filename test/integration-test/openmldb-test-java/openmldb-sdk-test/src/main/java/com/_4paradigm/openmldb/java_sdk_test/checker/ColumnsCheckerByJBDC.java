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

package com._4paradigm.openmldb.java_sdk_test.checker;


import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.Table;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class ColumnsCheckerByJBDC extends BaseChecker {
    public ColumnsCheckerByJBDC(ExpectDesc expect, OpenMLDBResult openMLDBResult) {
        super(expect, openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        log.info("column name check");
        List<String> expectColumns = expect.getColumns();
        if (expectColumns == null || expectColumns.size() == 0) {
            return;
        }
        List<String> columnNames = openMLDBResult.getColumnNames();
        Assert.assertEquals(expectColumns.size(),columnNames.size(), "Illegal schema size");
        for (int i = 0; i < expectColumns.size(); i++) {
            Assert.assertEquals(columnNames.get(i), Table.getColumnName(expectColumns.get(i)).replace(" ",""));
            // Assert.assertEquals(FesqlUtil.getColumnTypeByJDBC(columnTypes.get(i)),
            //         FesqlUtil.getColumnTypeByJDBC(Table.getColumnType(expectColumns.get(i))));
        }
    }
}
