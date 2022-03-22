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

package com._4paradigm.openmldb.java_sdk_test.entity;


import com._4paradigm.openmldb.test_common.model.CaseFile;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 3:19 PM
 */
@Data
public class FesqlDataProvider extends CaseFile {
    private static Logger logger = LoggerFactory.getLogger(FesqlDataProvider.class);
    public static final String FAIL_SQL_CASE= "FailSQLCase";

    public static FesqlDataProvider dataProviderGenerator(String caseFile) throws FileNotFoundException {
        try {
            Yaml yaml = new Yaml();
            FileInputStream testDataStream = new FileInputStream(caseFile);
            FesqlDataProvider testDateProvider = yaml.loadAs(testDataStream, FesqlDataProvider.class);
            return testDateProvider;
        } catch (Exception e) {
            logger.error("fail to load yaml: ", caseFile);
            e.printStackTrace();
            FesqlDataProvider nullDataProvider = new FesqlDataProvider();
            SQLCase failCase = new SQLCase();
            failCase.setDesc(FAIL_SQL_CASE);
            nullDataProvider.setCases(Lists.newArrayList(failCase));
            return nullDataProvider;
        }
    }


}

