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

package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.ReportLog;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/16 3:37 PM
 */
public abstract class BaseChecker implements Checker {
    protected FesqlResult fesqlResult;
    protected Map<String,FesqlResult> resultMap;
    protected ExpectDesc expect;
    protected ReportLog reportLog = ReportLog.of();

    public BaseChecker(ExpectDesc expect, FesqlResult fesqlResult){
        this.expect = expect;
        this.fesqlResult = fesqlResult;
    }

    public BaseChecker(FesqlResult fesqlResult,Map<String,FesqlResult> resultMap){
        this.fesqlResult = fesqlResult;
        this.resultMap = resultMap;
    }
}
