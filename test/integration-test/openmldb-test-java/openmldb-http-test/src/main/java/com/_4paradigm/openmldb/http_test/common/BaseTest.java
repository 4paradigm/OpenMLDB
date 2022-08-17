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


import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.provider.Yaml;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCaseFile;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.testng.annotations.DataProvider;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.util.List;

@Slf4j
public class BaseTest {
    protected Logger reportLog = new LogProxy(log);
    @DataProvider(name = "getCase")
    public Object[] getCaseByYaml(Method method) throws FileNotFoundException {
        String[] casePaths = method.getAnnotation(Yaml.class).filePaths();
        if (casePaths == null || casePaths.length == 0) {
            throw new RuntimeException("please add @Yaml");
        }
        List<RestfulCaseFile> caseFileList = RestfulCaseFileList.generatorCaseFileList(casePaths);
        List<RestfulCase> cases = RestfulCaseFileList.getCases(caseFileList);
        return cases.toArray();
    }
}
