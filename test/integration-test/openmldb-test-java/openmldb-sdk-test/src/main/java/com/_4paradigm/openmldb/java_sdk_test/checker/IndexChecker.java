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
import com._4paradigm.openmldb.test_common.bean.OpenMLDBIndex;
import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.TableIndex;
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
public class IndexChecker extends BaseChecker {
    private static final Logger logger = new LogProxy(log);
    public IndexChecker(ExpectDesc expect, OpenMLDBResult openMLDBResult){
        super(expect,openMLDBResult);
    }

    @Override
    public void check() throws Exception {
        logger.info("index check");
        List<TableIndex> expectIndexs = expect.getIdxs();
        List<OpenMLDBIndex> actualIndexs = openMLDBResult.getSchema().getIndexs();
        Assert.assertEquals(actualIndexs.size(),expectIndexs.size(),"index count 不一致");
        for(int i=0;i<expectIndexs.size();i++){
            TableIndex tableIndex = expectIndexs.get(i);
            OpenMLDBIndex openMLDBIndex = actualIndexs.get(i);
            String name = tableIndex.getName();
            List<String> keys = tableIndex.getKeys();
            String ts = tableIndex.getTs();
            String ttl = tableIndex.getTtl();
            String ttlType = tableIndex.getTtlType();
            if(StringUtils.isNotEmpty(name)){
                Assert.assertEquals(openMLDBIndex.getIndexName(),name,"index name 不一致");
            }
            if(StringUtils.isNotEmpty(ts)){
                Assert.assertEquals(openMLDBIndex.getTs(),ts,"index ts 不一致");
            }
            if(StringUtils.isNotEmpty(ttl)){
                Assert.assertEquals(openMLDBIndex.getTtl(),ttl,"index ttl 不一致");
            }
            if(StringUtils.isNotEmpty(ttlType)){
                Assert.assertEquals(openMLDBIndex.getTtlType(),ttlType,"index ttl type 不一致");
            }
            if(CollectionUtils.isNotEmpty(keys)){
                Assert.assertEquals(openMLDBIndex.getKeys(),keys,"index keys 不一致");
            }
        }
    }

}
