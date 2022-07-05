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
package com._4paradigm.openmldb.java_sdk_test.executor;


import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Map;

@Slf4j
public class ClusterCliExecutor extends CommandExecutor{
    private static final Logger logger = new LogProxy(log);
    public ClusterCliExecutor(SQLCase fesqlCase, SQLCaseType executorType) {
        super(fesqlCase, executorType);
    }

    public ClusterCliExecutor(SQLCase fesqlCase, Map<String, OpenMLDBInfo> openMLDBInfoMap, SQLCaseType executorType) {
        super(fesqlCase, openMLDBInfoMap, executorType);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("cluster-cli-unsupport")) {
            logger.info("skip case in cli mode: {}", fesqlCase.getDesc());
            return false;
        }
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("cluster-unsupport")) {
            logger.info("skip case , mode: {}", fesqlCase.getDesc());
            return false;
        }
        return super.verify();
    }
}
