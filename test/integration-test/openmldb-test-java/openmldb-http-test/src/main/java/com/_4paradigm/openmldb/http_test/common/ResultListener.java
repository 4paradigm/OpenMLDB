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
