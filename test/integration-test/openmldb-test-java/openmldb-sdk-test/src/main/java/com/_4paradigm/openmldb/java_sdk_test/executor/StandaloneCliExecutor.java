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


import com._4paradigm.openmldb.java_sdk_test.checker.Checker;
import com._4paradigm.openmldb.java_sdk_test.checker.CheckerStrategy;
import com._4paradigm.openmldb.java_sdk_test.checker.DiffVersionChecker;
import com._4paradigm.openmldb.java_sdk_test.command.OpenMLDBComamndFacade;
import com._4paradigm.openmldb.java_sdk_test.command.OpenMLDBCommandUtil;
import com._4paradigm.openmldb.java_sdk_test.command.OpenmlDBCommandFactory;
import com._4paradigm.openmldb.java_sdk_test.common.FedbGlobalVar;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import com._4paradigm.openmldb.test_common.bean.FEDBInfo;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class StandaloneCliExecutor extends CommandExecutor{
    private static final Logger logger = new LogProxy(log);
    public StandaloneCliExecutor(SQLCase fesqlCase, SQLCaseType executorType) {
        super(fesqlCase, executorType);
    }

    public StandaloneCliExecutor(SQLCase fesqlCase, Map<String, FEDBInfo> fedbInfoMap, SQLCaseType executorType) {
        super(fesqlCase, fedbInfoMap, executorType);
    }

    @Override
    public boolean verify() {
        if (null != fesqlCase.getMode() && fesqlCase.getMode().contains("standalone-unsupport")) {
            logger.info("skip case in cli mode: {}", fesqlCase.getDesc());
            return false;
        }
        return super.verify();
    }
}
