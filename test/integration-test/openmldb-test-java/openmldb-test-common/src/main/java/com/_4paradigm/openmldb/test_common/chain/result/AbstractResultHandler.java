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
package com._4paradigm.openmldb.test_common.chain.result;


import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.bean.SQLType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.Setter;

import java.sql.Statement;

@Setter
public abstract class AbstractResultHandler {
    private AbstractResultHandler nextHandler;

    public abstract boolean preHandle(SQLType sqlType);

    public abstract void onHandle(OpenMLDBResult openMLDBResult);

    public void doHandle(OpenMLDBResult openMLDBResult){
        String sql = openMLDBResult.getSql();
        SQLType sqlType = SQLType.parseSQLType(sql);
        if(preHandle(sqlType)){
            onHandle(openMLDBResult);
            return;
        }
        if(nextHandler!=null){
            nextHandler.doHandle(openMLDBResult);
            return;
        }
//        throw new IllegalArgumentException("result parse failed,not support sql type,sql:"+sql);
    }
}
