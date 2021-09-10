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
package com._4paradigm.openmldb.java_sdk_test.temp;


import com._4paradigm.openmldb.java_sdk_test.common.FedbClient;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import org.testng.annotations.Test;

import java.util.List;

public class TestDropTable {

    @Test
    public void testAll(){
        FedbClient fedbClient = new FedbClient("172.24.4.55:10000","/fedb");
        String dbName = "test_fedb";
        String sql = "show tables;";
        FesqlResult fesqlResult = FesqlUtil.select(fedbClient.getExecutor(), dbName, sql);
        List<List<Object>> result = fesqlResult.getResult();
        for(List<Object> list:result){
            System.out.println(list);
        }
    }
}
