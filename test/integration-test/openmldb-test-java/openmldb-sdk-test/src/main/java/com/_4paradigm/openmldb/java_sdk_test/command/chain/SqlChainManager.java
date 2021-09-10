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
package com._4paradigm.openmldb.java_sdk_test.command.chain;


import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;

public class SqlChainManager {
    private AbstractSQLHandler sqlHandler;
    private SqlChainManager() {
        sqlHandler = initHandler();
    }

    private AbstractSQLHandler initHandler(){
        QueryHandler queryHandler = new QueryHandler();
        UpdateHandler updateHandler = new UpdateHandler();
        queryHandler.setNextHandler(updateHandler);
        return queryHandler;
    }

    private static class ClassHolder {
        private static final SqlChainManager holder = new SqlChainManager();
    }

    public static SqlChainManager of() {
        return ClassHolder.holder;
    }
    public FesqlResult sql(FEDBInfo fedbInfo, String dbName, String sql){
        FesqlResult fesqlResult = sqlHandler.doHandle(fedbInfo, dbName, sql);
        return fesqlResult;
    }
}
