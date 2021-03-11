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

package com._4paradigm.fesql_auto_test.executor;

import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.sql.sdk.SqlExecutor;

import java.util.Map;

/**
 * @author zhaowei
 * @date 2021/2/5 3:31 PM
 */
public interface IExecutor {

    boolean verify();

    void run();

    void prepare() throws Exception;

    FesqlResult execute(String version,SqlExecutor executor);

    void check(FesqlResult fesqlResult,Map<String,FesqlResult> resultMap) throws Exception;

    void tearDown();
}
