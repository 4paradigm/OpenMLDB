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
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.testng.Assert;


/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class IndexCountChecker extends BaseChecker {
    private static final Logger logger = new LogProxy(log);
    public IndexCountChecker(ExpectDesc expect, OpenMLDBResult openMLDBResult){
        super(expect,openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        logger.info("index count check");
        int expectCount = expect.getIndexCount();
        int actual = openMLDBResult.getSchema().getIndexs().size();
        Assert.assertEquals(actual,expectCount,"index count验证失败");
    }

}
