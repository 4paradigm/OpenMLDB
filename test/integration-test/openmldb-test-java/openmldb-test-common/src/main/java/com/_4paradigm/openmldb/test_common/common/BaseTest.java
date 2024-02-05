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
package com._4paradigm.openmldb.test_common.common;


import com._4paradigm.openmldb.test_common.model.CaseFile;
import com._4paradigm.openmldb.test_common.model.OpenMLDBCaseFileList;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.ITest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;


@Slf4j
public class BaseTest implements ITest {
    private ThreadLocal<String> testName = new ThreadLocal<>();
    private int testNum = 0;

    public static String CaseNameFormat(SQLCase sqlCase) {
        return String.format("%s_%s_%s_%s",
                OpenMLDBGlobalVar.env,sqlCase.getCaseFileName(), sqlCase.getId(), sqlCase.getDesc());
    }

    @DataProvider(name = "getCase")
    public Object[] getCaseByYaml(Method method) throws FileNotFoundException {
        String[] casePaths = method.getAnnotation(Yaml.class).filePaths();
        if(casePaths==null||casePaths.length==0){
            throw new RuntimeException("please add @Yaml");
        }
        OpenMLDBCaseFileList dp = OpenMLDBCaseFileList.dataProviderGenerator(casePaths);
        Object[] caseArray = dp.getCases().toArray();
        log.info("caseArray.length:{}",caseArray.length);
        return caseArray;
    }

    @BeforeMethod
    public void BeforeMethod(Method method, Object[] testData) {
        ReportLog.of().clean();
        if(testData==null || testData.length==0) return;
        Assert.assertNotNull(
                testData[0], "fail to run openmldb test with null SQLCase: check yaml case");
        if (testData[0] instanceof SQLCase) {
            SQLCase sqlCase = (SQLCase) testData[0];
            log.info(sqlCase.getDesc());
            System.out.println(sqlCase.getDesc());
            Assert.assertNotEquals(CaseFile.FAIL_SQL_CASE,
                    sqlCase.getDesc(), "fail to run openmldb test with FAIL DATA PROVIDER SQLCase: check yaml case");
            testName.set(String.format("[%d]%s.%s", testNum, method.getName(), CaseNameFormat(sqlCase)));
        } else {
            testName.set(String.format("[%d]%s.%s", testNum, method.getName(), null == testData[0] ? "null" : testData[0].toString()));
        }
        testNum++;
    }
    @Override
    public String getTestName() {
        return testName.get();
    }
}
