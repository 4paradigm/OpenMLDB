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



import com._4paradigm.openmldb.java_sdk_test.command.OpenmlDBCommandFactory;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.CommandResultUtil;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com.google.common.base.Joiner;
import org.testng.collections.Lists;

import java.util.List;

public class ShowDeploymentsHandler extends AbstractSQLHandler{
    @Override
    public boolean preHandle(String sql) {
        String lowerSql = sql.toLowerCase();
        return lowerSql.startsWith("show deployments");
    }

    @Override
    public FesqlResult onHandle(OpenMLDBInfo openMLDBInfo, String dbName, String sql) {
        FesqlResult fesqlResult = new FesqlResult();
        List<String> result = OpenmlDBCommandFactory.runNoInteractive(openMLDBInfo,dbName,sql);
        boolean ok = CommandResultUtil.success(result);
        fesqlResult.setMsg(Joiner.on("\n").join(result));
        fesqlResult.setOk(ok);
        fesqlResult.setDbName(dbName);
        if (ok && result.size()>3) {
            fesqlResult.setDeployments(CommandResultUtil.parseDeployments(result));
        }else if(result.get(0).equals("Empty set")){
            fesqlResult.setDeployments(Lists.newArrayList());
        }
        return fesqlResult;
    }
}
