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


import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.PreAggTable;
import com._4paradigm.openmldb.test_common.util.DataUtil;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.collections.Lists;

import java.text.ParseException;
import java.util.Collections;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class PreAggListChecker extends BaseChecker {
    private SqlExecutor executor;

    public PreAggListChecker(ExpectDesc expect, OpenMLDBResult openMLDBResult) {
        super(expect, openMLDBResult);
    }

    public PreAggListChecker(SqlExecutor executor, ExpectDesc expect, OpenMLDBResult openMLDBResult){
        this(expect,openMLDBResult);
        this.executor = executor;
    }

    @Override
    public void check() throws ParseException {
        log.info("pre agg check");
//        if (CollectionUtils.isEmpty(expect.getPreAggList())) {
//            throw new RuntimeException("fail check pre agg list: PreAggTable is empty");
//        }
        String dbName = openMLDBResult.getDbName();
        String spName = openMLDBResult.getSpName();
        List<PreAggTable> preAggList = expect.getPreAggList();
        for(PreAggTable preAgg:preAggList) {
            String preAggTableName = preAgg.getName();
            String type = preAgg.getType();
            preAggTableName = SQLUtil.replaceDBNameAndSpName(dbName, spName, preAggTableName);
            String sql = String.format("select key,ts_start,ts_end,num_rows,agg_val,filter_key from %s", preAggTableName);
            OpenMLDBResult actualResult = SDKUtil.select(executor, "__PRE_AGG_DB", sql);
            List<List<Object>> actualRows = actualResult.getResult();
            actualRows.stream().forEach(l -> {
                Object o = DataUtil.parseBinary((String) l.get(4), type);
                l.set(4, o);
            });
            List expectColumns = Lists.newArrayList("string", "timestamp", "timestamp", "int", "string", "string");
            List<List<Object>> expectRows = DataUtil.convertRows(preAgg.getRows(), expectColumns);

            int index = 1;
            Collections.sort(expectRows, new RowsSort(index));
            Collections.sort(actualRows, new RowsSort(index));
            log.info("expect:{}", expectRows);
            log.info("actual:{}", actualRows);

            Assert.assertEquals(actualRows.size(), expectRows.size(), String.format("ResultChecker fail: expect size %d, real size %d", expectRows.size(), actualRows.size()));
            for (int i = 0; i < actualRows.size(); ++i) {
                List<Object> actual_list = actualRows.get(i);
                List<Object> expect_list = expectRows.get(i);
                Assert.assertEquals(actual_list.size(), expect_list.size(), String.format(
                        "ResultChecker fail at %dth row: expect row size %d, real row size %d", i, expect_list.size(), actual_list.size()));
                for (int j = 0; j < actual_list.size(); ++j) {
                    Object actual_val = actual_list.get(j);
                    Object expect_val = expect_list.get(j);
                    Assert.assertEquals(actual_val, expect_val, String.format(
                            "ResultChecker fail: row=%d column=%d expect=%s real=%s\nexpect %s\nreal %s",
                            i, j, expect_val, actual_val, expectRows, actualRows));
                }
            }
        }
    }

}
