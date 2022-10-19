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
package com._4paradigm.openmldb.test_common.command.chain;



import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFactory;
import com._4paradigm.openmldb.test_common.util.CommandResultUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com.google.common.base.Joiner;

import java.util.List;

public class ShowDeploymentHandler extends AbstractSQLHandler{
    @Override
    public boolean preHandle(String sql) {
        String lowerSql = sql.toLowerCase();
        return lowerSql.startsWith("show deployment ");
    }

    @Override
    public OpenMLDBResult onHandle(OpenMLDBInfo openMLDBInfo, String dbName, String sql) {
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        List<String> result = OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo,dbName,sql);
        boolean ok = CommandResultUtil.success(result);
        openMLDBResult.setMsg(Joiner.on("\n").join(result));
        openMLDBResult.setOk(ok);
        openMLDBResult.setDbName(dbName);
        if (ok && result.size()>9) {
            openMLDBResult.setDeployment(CommandResultUtil.parseDeployment(result));
        }
        return openMLDBResult;
    }
}
