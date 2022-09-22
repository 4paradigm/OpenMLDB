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


import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.util.HttpRequest;
import com._4paradigm.openmldb.test_common.util.Tool;
import com.jayway.jsonpath.JsonPath;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class OptionsChecker extends BaseChecker {
    private static String reg = "\\{(\\d+)\\}";

    public OptionsChecker(ExpectDesc expect, OpenMLDBResult openMLDBResult) {
        super(expect, openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        log.info("options check");
        String apiserverEndpoint = OpenMLDBGlobalVar.mainInfo.getApiServerEndpoints().get(0);
        String dbName = openMLDBResult.getDbName();
        String tableName = expect.getName();
        if(tableName.matches(reg)){
            int index = Integer.parseInt(tableName.substring(1,tableName.length()-1));
            tableName = openMLDBResult.getTableNames().get(index);
        }
        String url = String.format("http://%s/dbs/%s/tables/%s",apiserverEndpoint,dbName,tableName);
        Tool.sleep(3000);
        HttpResult httpResult = HttpRequest.get(url);
        String resultData = httpResult.getData();
        Object partitionNum = JsonPath.read(resultData, "$.table.partition_num");
        Object replicaNum = JsonPath.read(resultData, "$.table.replica_num");
        Map<String, Object> options = expect.getOptions();
        Assert.assertEquals(partitionNum,options.get("partitionNum"),"partitionNum不一致,resultData:"+resultData);
        Assert.assertEquals(replicaNum,options.get("replicaNum"),"replicaNum不一致,resultData:"+resultData);
    }
}
