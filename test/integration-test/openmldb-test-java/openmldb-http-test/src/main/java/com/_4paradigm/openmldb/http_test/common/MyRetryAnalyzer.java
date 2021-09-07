package com._4paradigm.openmldb.http_test.common;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

public class MyRetryAnalyzer implements IRetryAnalyzer {
    private int count = 1;
    private int max_count = 3;   // Failed test cases could be run 3 times at most
    @Override
    public boolean retry(ITestResult result) {
        System.out.println("Test case ："+result.getName()+"，retry time: "+count+"");
        if (count < max_count) {
            count++;
            return true;
        }
        return false;
    }
}