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
import com._4paradigm.openmldb.test_common.restful.model.Expect;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.List;

public class CheckerStrategy {

    public static List<Checker> build(RestfulCase restfulCase, HttpResult httpResult) {

        List<Checker> checkList = new ArrayList<>();
        if (null == restfulCase) {
            return checkList;
        }
        Expect expect = restfulCase.getExpect();
        if(MapUtils.isNotEmpty(expect.getData())){
            checkList.add(new DataChecker(httpResult,expect));
        }
//
//        checkList.add(new SuccessChecker(expect, fesqlResult));
//
//        if (CollectionUtils.isNotEmpty(expect.getColumns())) {
//            if(executorType==SQLCaseType.kSQLITE3 || executorType==SQLCaseType.kMYSQL){
//                checkList.add(new ColumnsCheckerByJBDC(expect, fesqlResult));
//            }else {
//                checkList.add(new ColumnsChecker(expect, fesqlResult));
//            }
//        }
//        if (!expect.getRows().isEmpty()) {
//            if(executorType==SQLCaseType.kSQLITE3){
//                checkList.add(new ResultCheckerByJDBC(expect, fesqlResult));
//            }else {
//                checkList.add(new ResultChecker(expect, fesqlResult));
//            }
//        }
//
//        if (expect.getCount() >= 0) {
//            checkList.add(new CountChecker(expect, fesqlResult));
//        }
        return checkList;
    }

}
