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

package com._4paradigm.openmldb.java_sdk_test.entity;


import com._4paradigm.openmldb.test_common.model.CaseFile;
import com._4paradigm.openmldb.test_common.util.DataUtil;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.List;

public class FesqlDataProviderTest {

    @Test
    public void getDataProviderTest() throws FileNotFoundException {
        CaseFile provider = CaseFile.parseCaseFile("/yaml/rtidb_demo.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(3, provider.getCases().size());

        SQLCase sqlCase = provider.getCases().get(0);
        Assert.assertEquals(2, sqlCase.getInputs().size());
    }

    @Test
    public void getInsertTest() throws FileNotFoundException {
        CaseFile provider = CaseFile.parseCaseFile("/yaml/rtidb_demo2.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(1, provider.getCases().size());
        SQLCase sqlCase = provider.getCases().get(0);
        Assert.assertEquals(2, sqlCase.getInputs().size());

        InputDesc input = sqlCase.getInputs().get(0);
        Assert.assertEquals(input.extractInsert(), "insert into " + input.getName() + " values\n" +
                "('aa',2,3,1590738989000L),\n" +
                "(null,null,null,1590738990000L);");
    }

    @Test
    public void getInserstTest() throws FileNotFoundException {
        CaseFile provider = CaseFile.parseCaseFile("/yaml/rtidb_demo2.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(1, provider.getCases().size());
        SQLCase sqlCase = provider.getCases().get(0);
        Assert.assertEquals(2, sqlCase.getInputs().size());

        InputDesc input = sqlCase.getInputs().get(0);
        Assert.assertEquals(input.extractInserts(),
                Lists.newArrayList("insert into " + input.getName() + " values\n" +
                                "('aa',2,3,1590738989000L);",
                        "insert into " + input.getName() + " values\n" +
                                "(null,null,null,1590738990000L);"
                ));
    }

    @Test
    public void getCreateTest() throws FileNotFoundException {
        CaseFile provider = CaseFile.parseCaseFile("/yaml/rtidb_demo2.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(1, provider.getCases().size());
        SQLCase sqlCase = provider.getCases().get(0);
        Assert.assertEquals(2, sqlCase.getInputs().size());

        InputDesc input = sqlCase.getInputs().get(0);
        Assert.assertEquals("create table " + input.getName() + "(\n" +
                "c1 string,\n" +
                "c2 int,\n" +
                "c3 bigint,\n" +
                "c4 timestamp,\n" +
                "index(key=(c1),ts=c4));", input.extractCreate());
    }


    @Test
    public void converRowsTest() throws ParseException, FileNotFoundException {
        CaseFile provider = CaseFile.parseCaseFile("/yaml/rtidb_demo.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(3, provider.getCases().size());
        SQLCase sqlCase = provider.getCases().get(0);
        Assert.assertEquals(2, sqlCase.getInputs().size());
        List<List<Object>> expect = DataUtil.convertRows(sqlCase.getExpect().getRows(),
                sqlCase.getExpect().getColumns());
    }
}
