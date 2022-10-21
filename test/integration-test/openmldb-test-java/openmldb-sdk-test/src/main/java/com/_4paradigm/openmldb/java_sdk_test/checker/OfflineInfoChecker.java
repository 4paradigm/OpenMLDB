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


import com._4paradigm.openmldb.test_common.bean.OfflineInfo;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBIndex;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.TableIndex;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.testng.Assert;

import java.util.List;


/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class OfflineInfoChecker extends BaseChecker {
    private static final Logger logger = new LogProxy(log);
    public OfflineInfoChecker(ExpectDesc expect, OpenMLDBResult openMLDBResult){
        super(expect,openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        logger.info("index check");
        OfflineInfo expectOfflineInfo = expect.getOfflineInfo();
        OfflineInfo actualOfflineInfo = openMLDBResult.getSchema().getOfflineInfo();
        String expectPath = expectOfflineInfo.getPath();
        expectPath = SQLUtil.formatSql(expectPath,openMLDBResult.getTableNames());
        expectPath = SQLUtil.formatSql(expectPath);
        Assert.assertEquals(actualOfflineInfo.getPath(),expectPath,"offline path 不一致");
    }

}
