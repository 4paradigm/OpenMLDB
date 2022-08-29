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

package com._4paradigm.openmldb.java_sdk_test.report;

import com._4paradigm.openmldb.java_sdk_test.common.OpenMLDBConfig;
import com._4paradigm.openmldb.test_common.common.ReportLog;
import io.qameta.allure.Attachment;
import org.testng.IHookCallBack;
import org.testng.IHookable;
import org.testng.ITestResult;
import org.yaml.snakeyaml.Yaml;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/15 8:46 AM
 */
public class AddAttachmentListener implements IHookable {
    private Yaml yaml = new Yaml();
    @Attachment(value = "test-log")
    public String addLog(){
        StringBuilder sb = new StringBuilder();
        List<String> logs = ReportLog.of().getLogs();
        for(String log:logs){
            sb.append(log).append("\n");
        }
        return sb.toString();
    }

    @Attachment(value = "test-case")
    public String addCase(Object obj){
        String dump = yaml.dump(obj);
        return dump;
    }

    @Override
    public void run(IHookCallBack callBack, ITestResult testResult) {
        callBack.runTestMethod(testResult);
        if(OpenMLDBConfig.ADD_REPORT_LOG&&testResult.getThrowable()!=null) {
            Object[] parameters = testResult.getParameters();
            if(parameters!=null&&parameters.length>0) {
                Object parameter = testResult.getParameters()[0];
                addCase(parameter);
            }
            addLog();
        }
    }
}
