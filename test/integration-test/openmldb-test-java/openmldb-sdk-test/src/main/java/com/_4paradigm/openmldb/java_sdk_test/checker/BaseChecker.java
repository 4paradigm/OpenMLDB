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

package com._4paradigm.openmldb.java_sdk_test.checker;


import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/16 3:37 PM
 */
public abstract class BaseChecker implements Checker {
    protected OpenMLDBResult openMLDBResult;
    protected Map<String, OpenMLDBResult> resultMap;
    protected ExpectDesc expect;

    public BaseChecker(ExpectDesc expect, OpenMLDBResult openMLDBResult){
        this.expect = expect;
        this.openMLDBResult = openMLDBResult;
    }

    public BaseChecker(OpenMLDBResult openMLDBResult, Map<String, OpenMLDBResult> resultMap){
        this.openMLDBResult = openMLDBResult;
        this.resultMap = resultMap;
    }
}
