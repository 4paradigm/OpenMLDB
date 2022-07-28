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
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class CheckerStrategy {

    public static List<Checker> build(SQLCase fesqlCase, OpenMLDBResult fesqlResult, SQLCaseType executorType) {
        List<Checker> checkList = new ArrayList<>();
        if (null == fesqlCase) {
            return checkList;
        }
        ExpectDesc expect = fesqlCase.getOnlineExpectByType(executorType);

        checkList.add(new SuccessChecker(expect, fesqlResult));

        if (CollectionUtils.isNotEmpty(expect.getColumns())) {
            if(executorType==SQLCaseType.kSQLITE3 || executorType==SQLCaseType.kMYSQL){
                checkList.add(new ColumnsCheckerByJBDC(expect, fesqlResult));
            }else if(executorType==SQLCaseType.kCLI||executorType==SQLCaseType.kStandaloneCLI||executorType==SQLCaseType.kClusterCLI){
                checkList.add(new ColumnsCheckerByCli(expect, fesqlResult));
            }else {
                checkList.add(new ColumnsChecker(expect, fesqlResult));
            }
        }
        if (!expect.getRows().isEmpty()) {
            if(executorType==SQLCaseType.kSQLITE3){
                checkList.add(new ResultCheckerByJDBC(expect, fesqlResult));
            }else if(executorType==SQLCaseType.kCLI||executorType==SQLCaseType.kStandaloneCLI||executorType==SQLCaseType.kClusterCLI){
                checkList.add(new ResultCheckerByCli(expect, fesqlResult));
            }else {
                checkList.add(new ResultChecker(expect, fesqlResult));
            }
        }

        if (expect.getCount() >= 0) {
            checkList.add(new CountChecker(expect, fesqlResult));
        }
        if(MapUtils.isNotEmpty(expect.getOptions())){
            checkList.add(new OptionsChecker(expect, fesqlResult));
        }
        if(CollectionUtils.isNotEmpty(expect.getIdxs())){
            checkList.add(new IndexChecker(expect, fesqlResult));
        }
        if (expect.getIndexCount() >= 0) {
            checkList.add(new IndexCountChecker(expect, fesqlResult));
        }
        if(expect.getDeployment()!=null){
            checkList.add(new DeploymentCheckerByCli(expect, fesqlResult));
        }
        if(expect.getDeploymentContains()!=null){
            checkList.add(new DeploymentContainsCheckerByCli(expect, fesqlResult));
        }
        if(expect.getDeploymentCount()>=0){
            checkList.add(new DeploymentCountCheckerByCli(expect, fesqlResult));
        }
        if(expect.getCat()!=null){
            checkList.add(new CatCheckerByCli(expect, fesqlResult));
        }
        if(StringUtils.isNotEmpty(expect.getMsg())){
            checkList.add(new MessageChecker(expect, fesqlResult));
        }
        return checkList;
    }

}
