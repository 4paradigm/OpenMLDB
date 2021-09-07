package com._4paradigm.openmldb.http_test.common;

import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.util.Iterator;

public class ResultListener extends TestListenerAdapter {
    @Override
    public void onTestFailure(ITestResult tr) {
        if(tr.getMethod().getCurrentInvocationCount()==1)
        {
            super.onTestFailure(tr);
            return;
        }

        processSkipResult(tr);
        super.onTestFailure(tr);
    }
    @Override
    public void onTestSuccess(ITestResult tr) {
        if(tr.getMethod().getCurrentInvocationCount()==1)
        {
            super.onTestSuccess(tr);
            return;
        }
        processSkipResult(tr);
        super.onTestSuccess(tr);
    }
    // Remove all the dup Skipped results
    public void processSkipResult(ITestResult tr)
    {
        ITestContext iTestContext = tr.getTestContext();
        Iterator<ITestResult> processResults = iTestContext.getSkippedTests().getAllResults().iterator();
        while (processResults.hasNext()) {
            ITestResult skippedTest = (ITestResult) processResults.next();
            if (skippedTest.getMethod().getMethodName().equalsIgnoreCase(tr.getMethod().getMethodName()) ) {
                processResults.remove();
            }
        }
    }
}
