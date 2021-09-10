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
package com._4paradigm.openmldb.http_test.check;


import com._4paradigm.openmldb.test_common.common.Checker;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.restful.model.Expect;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

@Slf4j
public abstract class BaseChecker implements Checker {
    protected HttpResult httpResult;
    protected Expect expect;

    protected Logger logger = new LogProxy(log);

    public BaseChecker(HttpResult httpResult, Expect expect) {
        this.httpResult = httpResult;
        this.expect = expect;
    }
}
