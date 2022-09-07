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

package com._4paradigm.openmldb.java_sdk_test.standalone.v030;


import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFacade;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.java_sdk_test.common.StandaloneTest;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.java_sdk_test.executor.ExecutorFactory;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author zhaowei
 * @date 2020/6/11 2:53 PM
 */
@Slf4j
@Feature("CLI-DML")
public class DMLTest extends StandaloneTest {

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/dml/test_insert.yaml")
    @Story("insert")
    public void testInsert(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/dml/multi_insert.yaml")
    @Story("insert-multi")
    public void testInsertMulti(SQLCase testCase){
        ExecutorFactory.build(testCase, SQLCaseType.kStandaloneCLI).run();
    }

    @Story("insert-multi")
    // @Test
    public void testInsertMulti1000(){
        String createSql = "create table auto_multi_insert_1000 (id int not null,\n" +
                "          c1 int not null,\n" +
                "          c2 smallint not null,\n" +
                "          c3 float not null,\n" +
                "          c4 double not null,\n" +
                "          c5 bigint not null,\n" +
                "          c6 string not null,\n" +
                "          c7 timestamp not null,\n" +
                "          c8 date not null,\n" +
                "          c9 bool not null,\n" +
                "          index(key=(c1), ts=c5));";
        OpenMLDBCommandFacade.sql(OpenMLDBGlobalVar.mainInfo, OpenMLDBGlobalVar.dbName,createSql);
        StringBuilder sb = new StringBuilder("insert into auto_multi_insert_1000 values ");
        int total = 1000;
        for(int i=0;i<total;i++){
            sb.append("("+i+", 1, 2, 3.3f, 4.4, 5L, \"aa\", 12345678L, \"2020-05-21\", true),");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(";");
        OpenMLDBCommandFacade.sql(OpenMLDBGlobalVar.mainInfo, OpenMLDBGlobalVar.dbName,sb.toString());
        String query = "select * from auto_multi_insert_1000;";
        OpenMLDBResult result = OpenMLDBCommandFacade.sql(OpenMLDBGlobalVar.mainInfo, OpenMLDBGlobalVar.dbName, query);
        Assert.assertEquals(total,result.getCount());
    }

    //pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/dml/test_insert.yaml")
    @Story("insert")
    public void testInsertSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

    //pass
    @Test(dataProvider = "getCase")
    @Yaml(filePaths = "function/dml/multi_insert.yaml")
    @Story("insert-multi")
    public void testInsertMultiSDK(SQLCase testCase){
        ExecutorFactory.build(executor,testCase, SQLCaseType.kBatch).run();
    }

}
