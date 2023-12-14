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
package com._4paradigm.openmldb.java_sdk_test.executor;


import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.collections.Lists;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/13 10:18 AM
 */
@Slf4j
public abstract class BaseExecutor implements IExecutor{
//    protected static final log log = new LogProxy(log);
    protected SQLCase sqlCase;
    protected SQLCaseType executorType;
    protected String dbName;
    protected List<String> tableNames = Lists.newArrayList();
    protected OpenMLDBResult mainResult;

    @Override
    public void run() {
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        System.out.println(className+"."+methodName+":"+ sqlCase.getCaseFileName()+":"+ sqlCase.getDesc() + " Begin!");
        boolean verify = false;
        try {
            verify = verify();
            if(!verify) return;
            if (null == sqlCase) {
                Assert.fail("executor run with null case");
                return;
            }
            prepare();
            execute();
            check();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(className+"."+methodName+":"+ sqlCase.getDesc() + " FAIL!");
            Assert.fail("executor run with exception "+sqlCase.getDesc());
        }finally {
            if(verify) {
                tearDown();
            }
            System.out.println(className+"."+methodName+":"+ sqlCase.getDesc() + " DONE!");
        }
    }
}
