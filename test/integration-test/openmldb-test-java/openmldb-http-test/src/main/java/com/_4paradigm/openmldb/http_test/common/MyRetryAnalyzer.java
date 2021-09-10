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