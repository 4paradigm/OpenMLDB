package com._4paradigm.fesql_auto_test.report;

import com._4paradigm.fesql_auto_test.util.ReportLog;
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
        if(testResult.getThrowable()!=null) {
            Object parameter = testResult.getParameters()[0];
            // addCase(parameter);
            // addLog();
        }
    }
}
