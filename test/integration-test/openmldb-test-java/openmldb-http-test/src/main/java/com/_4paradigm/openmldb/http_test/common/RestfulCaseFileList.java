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

import com._4paradigm.openmldb.http_test.config.FedbRestfulConfig;
import com._4paradigm.openmldb.http_test.util.CaseUtil;
import com._4paradigm.openmldb.test_common.provider.YamlUtil;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCaseFile;
import com._4paradigm.openmldb.test_common.util.Tool;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class RestfulCaseFileList {

    public static List<RestfulCase> getCases(List<RestfulCaseFile> caseFileList) {
        List<RestfulCase> cases = new ArrayList<RestfulCase>();

        for (RestfulCaseFile caseFile : caseFileList) {
            for (RestfulCase restfulCase : caseFile.getCases(FedbRestfulConfig.FESQL_CASE_LEVELS)) {
                if (!StringUtils.isEmpty(FedbRestfulConfig.FESQL_CASE_NAME) &&
                        !FedbRestfulConfig.FESQL_CASE_NAME.equals(CaseUtil.caseNameFormat(restfulCase))) {
                    continue;
                }
                if (!StringUtils.isEmpty(FedbRestfulConfig.FESQL_CASE_ID)
                        && !FedbRestfulConfig.FESQL_CASE_ID.equals(restfulCase.getCaseId())) {
                    continue;
                }
                if (!StringUtils.isEmpty(FedbRestfulConfig.FESQL_CASE_DESC)
                        && !FedbRestfulConfig.FESQL_CASE_DESC.equals(restfulCase.getDesc())) {
                    continue;
                }
                cases.add(restfulCase);
            }
        }
        return cases;
    }

    public static List<RestfulCaseFile> generatorCaseFileList(String[] caseFiles) throws FileNotFoundException {
        List<RestfulCaseFile> caseFileList = new ArrayList<>();
        for (String caseFile : caseFiles) {
            if (!StringUtils.isEmpty(FedbRestfulConfig.FESQL_CASE_PATH)
                    && !FedbRestfulConfig.FESQL_CASE_PATH.equals(caseFile)) {
                continue;
            }
            String casePath = Tool.getCasePath(FedbRestfulConfig.YAML_CASE_BASE_DIR, caseFile);
            File file = new File(casePath);
            if (!file.exists()) {
                continue;
            }
            if (file.isFile()) {
                caseFileList.add(YamlUtil.getObject(casePath, RestfulCaseFile.class));
            } else {
                File[] files = file.listFiles(f -> f.getName().endsWith(".yaml"));
                for (File f : files) {
                    caseFileList.add(YamlUtil.getObject(f.getAbsolutePath(), RestfulCaseFile.class));
                }
            }
        }
        return caseFileList;
    }

}
