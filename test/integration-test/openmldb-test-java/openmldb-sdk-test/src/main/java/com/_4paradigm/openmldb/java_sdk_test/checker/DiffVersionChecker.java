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


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2021/2/5 5:23 PM
 */
@Slf4j
public class DiffVersionChecker extends BaseChecker{

    public DiffVersionChecker(FesqlResult fesqlResult, Map<String,FesqlResult> resultMap){
        super(fesqlResult,resultMap);
    }

    @Override
    public void check() throws Exception {
        log.info("diff version check");
        reportLog.info("diff version check");
        resultMap.entrySet().stream().forEach(e->{
            String version = e.getKey();
            FesqlResult result = e.getValue();
            Assert.assertTrue(fesqlResult.equals(result),"版本结果对比不一致\nmainVersion:\n"+fesqlResult+"\nversion:"+version+"\n"+result);
        });
    }
}
