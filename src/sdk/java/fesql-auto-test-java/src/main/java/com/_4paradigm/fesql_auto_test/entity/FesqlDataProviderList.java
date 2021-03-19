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

package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.hybridse.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.HybridSEConfig;
import com._4paradigm.fesql_auto_test.common.FesqlTest;
import com._4paradigm.fesql_auto_test.util.Tool;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class FesqlDataProviderList {
    private List<FesqlDataProvider> dataProviderList = new ArrayList<FesqlDataProvider>();

    public List<SQLCase> getCases() {
        List<SQLCase> cases = new ArrayList<SQLCase>();

        for (FesqlDataProvider dataProvider : dataProviderList) {
            for (SQLCase sqlCase : dataProvider.getCases(HybridSEConfig.FESQL_CASE_LEVELS)) {
                if (!StringUtils.isEmpty(HybridSEConfig.FESQL_CASE_NAME) &&
                        !HybridSEConfig.FESQL_CASE_NAME.equals(FesqlTest.CaseNameFormat(sqlCase))) {
                    continue;
                }
                if (!StringUtils.isEmpty(HybridSEConfig.FESQL_CASE_ID)
                        && !HybridSEConfig.FESQL_CASE_ID.equals(sqlCase.getId())) {
                    continue;
                }
                if (!StringUtils.isEmpty(HybridSEConfig.FESQL_CASE_DESC)
                        && !HybridSEConfig.FESQL_CASE_DESC.equals(sqlCase.getDesc())) {
                    continue;
                }
                cases.add(sqlCase);
            }
        }
        return cases;
    }

    public static FesqlDataProviderList dataProviderGenerator(String[] caseFiles) throws FileNotFoundException {

        FesqlDataProviderList fesqlDataProviderList = new FesqlDataProviderList();
        for (String caseFile : caseFiles) {
            if (!StringUtils.isEmpty(HybridSEConfig.FESQL_CASE_PATH)
                    && !HybridSEConfig.FESQL_CASE_PATH.equals(caseFile)) {
                continue;
            }
            String casePath = Tool.getCasePath(caseFile);
            File file = new File(casePath);
            if (!file.exists()) {
                continue;
            }
            if (file.isFile()) {
                fesqlDataProviderList.dataProviderList.add(FesqlDataProvider.dataProviderGenerator(casePath));
            } else {
                File[] files = file.listFiles(f -> f.getName().endsWith(".yaml"));
                for (File f : files) {
                    fesqlDataProviderList.dataProviderList.add(FesqlDataProvider.dataProviderGenerator(f.getAbsolutePath()));
                }
            }
        }
        return fesqlDataProviderList;
    }

}
