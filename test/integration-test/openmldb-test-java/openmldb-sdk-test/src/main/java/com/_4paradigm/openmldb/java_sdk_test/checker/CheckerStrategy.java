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


import com._4paradigm.openmldb.sdk.SqlExecutor;
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

    public static List<Checker> build(SqlExecutor executor,SQLCase sqlCase, OpenMLDBResult openMLDBResult, SQLCaseType executorType) {
        List<Checker> checkList = new ArrayList<>();
        if (null == sqlCase) {
            return checkList;
        }
        ExpectDesc expect = sqlCase.getOnlineExpectByType(executorType);
        if (null == expect) {
            return checkList;
        }
        checkList.add(new SuccessChecker(expect, openMLDBResult));

        if (CollectionUtils.isNotEmpty(expect.getColumns())) {
            if(executorType==SQLCaseType.kSQLITE3 || executorType==SQLCaseType.kMYSQL){
                checkList.add(new ColumnsCheckerByJBDC(expect, openMLDBResult));
            }else if(executorType==SQLCaseType.kCLI||executorType==SQLCaseType.kStandaloneCLI||executorType==SQLCaseType.kClusterCLI){
                checkList.add(new ColumnsCheckerByCli(expect, openMLDBResult));
            }else if(executorType==SQLCaseType.KOfflineJob){
                checkList.add(new ColumnsCheckerByOffline(expect, openMLDBResult));
            }else {
                checkList.add(new ColumnsChecker(expect, openMLDBResult));
            }
        }
        if (!expect.getRows().isEmpty()) {
            if(executorType==SQLCaseType.kSQLITE3){
                checkList.add(new ResultCheckerByJDBC(expect, openMLDBResult));
            }else if(executorType==SQLCaseType.kCLI||executorType==SQLCaseType.kStandaloneCLI||executorType==SQLCaseType.kClusterCLI){
                checkList.add(new ResultCheckerByCli(expect, openMLDBResult));
            }else if(executorType==SQLCaseType.KOfflineJob){
                checkList.add(new ResultCheckerByOffline(expect, openMLDBResult));
            }else {
                checkList.add(new ResultChecker(expect, openMLDBResult));
            }
        }

        if (expect.getCount() >= 0) {
            checkList.add(new CountChecker(expect, openMLDBResult));
        }
        if(MapUtils.isNotEmpty(expect.getOptions())){
            checkList.add(new OptionsChecker(expect, openMLDBResult));
        }
        if(CollectionUtils.isNotEmpty(expect.getIdxs())){
            checkList.add(new IndexChecker(expect, openMLDBResult));
        }
        if (expect.getIndexCount() >= 0) {
            checkList.add(new IndexCountChecker(expect, openMLDBResult));
        }
        if(expect.getDeployment()!=null){
            checkList.add(new DeploymentCheckerByCli(expect, openMLDBResult));
        }
        if(expect.getDeploymentContains()!=null){
            checkList.add(new DeploymentContainsCheckerByCli(expect, openMLDBResult));
        }
        if(expect.getDeploymentCount()>=0){
            checkList.add(new DeploymentCountCheckerByCli(expect, openMLDBResult));
        }
        if(expect.getCat()!=null){
            checkList.add(new CatCheckerByCli(expect, openMLDBResult));
        }
        if(StringUtils.isNotEmpty(expect.getMsg())){
            checkList.add(new MessageChecker(expect, openMLDBResult));
        }
        if(expect.getPreAgg()!=null){
            checkList.add(new PreAggChecker(executor, expect, openMLDBResult));
        }
        if(CollectionUtils.isNotEmpty(expect.getPreAggList())){
            checkList.add(new PreAggListChecker(executor, expect, openMLDBResult));
        }
        if(expect.getOfflineInfo()!=null){
            checkList.add(new OfflineInfoChecker(expect, openMLDBResult));
        }
        if(CollectionUtils.isNotEmpty(expect.getOfflineColumns())){
            checkList.add(new ColumnsCheckerByOffline(expect, openMLDBResult));
        }
        if(CollectionUtils.isNotEmpty(expect.getOfflineRows())){
            checkList.add(new ResultCheckerByOffline(expect, openMLDBResult));
        }
        return checkList;
    }

}
