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
import com._4paradigm.openmldb.test_common.util.DataUtil;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.Table;
import com._4paradigm.openmldb.test_common.util.SchemaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/10 6:22 PM
 */
@Slf4j
public class ResultCheckerByJDBC extends BaseChecker {

    public ResultCheckerByJDBC(ExpectDesc expect, OpenMLDBResult openMLDBResult) {
        super(expect, openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        log.info("result check");
        if (expect.getColumns().isEmpty()) {
            throw new RuntimeException("fail check result: columns are empty");
        }
        List<List<Object>> expectRows = DataUtil.convertRows(expect.getRows(),
                expect.getColumns());
        List<List<Object>> actual = openMLDBResult.getResult();

        String orderName = expect.getOrder();
        if (StringUtils.isNotEmpty(orderName)) {
            int index = SchemaUtil.getIndexByColumnName(openMLDBResult.getColumnNames(),orderName);
            Collections.sort(expectRows, new RowsSort(index));
            Collections.sort(actual, new RowsSort(index));
        }

        log.info("expect:{}", expectRows);
        log.info("actual:{}", actual);
        Assert.assertEquals(actual.size(), expectRows.size(),
                String.format("ResultChecker fail: expect size %d, real size %d", expectRows.size(), actual.size()));
        for (int i = 0; i < actual.size(); ++i) {
            List<Object> actual_list = actual.get(i);
            List<Object> expect_list = expectRows.get(i);
            Assert.assertEquals(actual_list.size(), expect_list.size(), String.format(
                    "ResultChecker fail at %dth row: expect row size %d, real row size %d",
                    i, expect_list.size(), actual_list.size()));
            for (int j = 0; j < actual_list.size(); ++j) {
                Object actual_val = actual_list.get(j);
                Object expect_val = expect_list.get(j);

                if(String.valueOf(expect_val).equals("NaN")){
                    expect_val = null;
                }

                if(expect_val != null && expect_val instanceof Boolean){
                    actual_val = actual_val.equals(0)?false:true;
                    Assert.assertEquals(String.valueOf(actual_val), String.valueOf(expect_val), String.format(
                            "ResultChecker fail: row=%d column=%d expect=%s real=%s\nexpect %s\nreal %s",
                            i, j, expect_val, actual_val,
                            Table.getTableString(expect.getColumns(), expectRows),
                            openMLDBResult.toString()));
                }else if (actual_val != null && actual_val instanceof Double) {
                    // Assert.assertTrue(expect_val != null && expect_val instanceof Double);
                    if(expect_val instanceof Float){
                        expect_val = ((Float)expect_val).doubleValue();
                    }else if(expect_val instanceof Timestamp){
                        expect_val = (double)((Timestamp)expect_val).getTime();
                    }else if(expect_val instanceof String){
                        expect_val = Double.parseDouble((String)expect_val);
                    }
                    Assert.assertEquals(
                            (Double) actual_val, (Double) expect_val, 1e-4,
                            String.format("ResultChecker fail: row=%d column=%d expect=%s real=%s\nexpect %s\nreal %s",
                                    i, j, expect_val, actual_val,
                                    Table.getTableString(expect.getColumns(), expectRows),
                                    openMLDBResult.toString())
                    );

                } else if(expect_val != null && expect_val instanceof Timestamp){
                    expect_val = ((Timestamp)expect_val).getTime();
                    Assert.assertEquals(String.valueOf(actual_val), String.valueOf(expect_val), String.format(
                            "ResultChecker fail: row=%d column=%d expect=%s real=%s\nexpect %s\nreal %s",
                            i, j, expect_val, actual_val,
                            Table.getTableString(expect.getColumns(), expectRows),
                            openMLDBResult.toString()));
                } else{
                    Assert.assertEquals(String.valueOf(actual_val), String.valueOf(expect_val), String.format(
                            "ResultChecker fail: row=%d column=%d expect=%s real=%s\nexpect %s\nreal %s",
                            i, j, expect_val, actual_val,
                            Table.getTableString(expect.getColumns(), expectRows),
                            openMLDBResult.toString()));

                }
            }
        }
    }
}
