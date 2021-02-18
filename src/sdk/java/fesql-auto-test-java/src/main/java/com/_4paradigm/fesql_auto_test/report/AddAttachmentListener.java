package com._4paradigm.fesql_auto_test.report;

import com._4paradigm.fesql_auto_test.util.ReportLog;
import io.qameta.allure.Attachment;
import org.testng.IHookCallBack;
import org.testng.IHookable;
import org.testng.ITestResult;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/2/15 8:46 AM
 */
public class AddAttachmentListener implements IHookable {
    @Attachment(value = "test-log")
    public String addLog(){
        StringBuilder sb = new StringBuilder();
        List<String> logs = ReportLog.of().getLogs();
        for(String log:logs){
            sb.append(log).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void run(IHookCallBack callBack, ITestResult testResult) {
        callBack.runTestMethod(testResult);
        if(testResult.getThrowable()!=null) {
            addLog();
        }
    }
}
