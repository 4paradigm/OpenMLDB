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
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class DeploymentCheckerByCli extends BaseChecker {
    public DeploymentCheckerByCli(ExpectDesc expect, OpenMLDBResult openMLDBResult) {
        super(expect, openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        log.info("deployment check");
        OpenmldbDeployment expectDeployment = expect.getDeployment();
        String name = expectDeployment.getName();
        name = SQLUtil.formatSql(name, openMLDBResult.getTableNames());
        expectDeployment.setName(name);
        String sql = expectDeployment.getSql();
        sql = SQLUtil.formatSql(sql, openMLDBResult.getTableNames());
        expectDeployment.setSql(sql);
        if (expectDeployment == null) {
            return;
        }
        OpenmldbDeployment actualDeployment = openMLDBResult.getDeployment();
        Assert.assertEquals(actualDeployment,expectDeployment);
    }
}
