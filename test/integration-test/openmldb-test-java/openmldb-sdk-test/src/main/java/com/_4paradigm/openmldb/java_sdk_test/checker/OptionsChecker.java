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


import com._4paradigm.openmldb.java_sdk_test.common.FedbGlobalVar;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.restful.util.HttpRequest;
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

    public OptionsChecker(ExpectDesc expect, FesqlResult fesqlResult) {
        super(expect, fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("options check");
        reportLog.info("options check");
        String apiserverEndpoint = FedbGlobalVar.mainInfo.getApiServerEndpoints().get(0);
        String dbName = fesqlResult.getDbName();
        String tableName = expect.getName();
        if(tableName.matches(reg)){
            int index = Integer.parseInt(tableName.substring(1,tableName.length()-1));
            tableName = fesqlResult.getTableNames().get(index);
        }
        String url = String.format("http://%s/dbs/%s/tables/%s",apiserverEndpoint,dbName,tableName);
        HttpResult httpResult = HttpRequest.get(url);
        String resultData = httpResult.getData();
        Object partitionNum = JsonPath.read(resultData, "$.table.partition_num");
        Object replicaNum = JsonPath.read(resultData, "$.table.replica_num");
        Map<String, Object> options = expect.getOptions();
        Assert.assertEquals(options.get("partitionNum"),partitionNum,"partitionNum不一致");
        Assert.assertEquals(options.get("replicaNum"),replicaNum,"replicaNum不一致");
    }
}
