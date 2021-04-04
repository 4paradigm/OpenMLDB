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

package com._4paradigm.hybridse.sdk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com._4paradigm.hybridse.sdk.DDLEngine.genDDL;
import static com._4paradigm.hybridse.sdk.DDLEngine.sql2Feconfig;

public class DDLEngineTest {
    private static final Logger logger = LoggerFactory.getLogger(DDLEngineTest.class);

    @DataProvider(name = "build_more_index")
    public Object[][] getSqlScript() {
        return new Object[][] {
                 new Object[] {
                         "support small short smallint",
                         "ddl/ut/type.json",
                         "ddl/ut/type.txt",
                         1,
                         2,
                         "create table `main`(\n" +
                                 "`col1` smallint,\n" +
                                 "`col2` int,\n" +
                                 "`col3` bigint,\n" +
                                 "`col4` float,\n" +
                                 "`col5` double,\n" +
                                 "`col6` bool,\n" +
                                 "`col7` string,\n" +
                                 "`col8` timestamp,\n" +
                                 "`col9` date,\n" +
                                 "index(key=(`col2`), ttl=1, ttl_type=latest)\n" +
                                 ") replicanum=1, partitionnum=2 ;\n"
                 },
                new Object[] {
                        "加法 和 乘法 输出列名有重复",
                        "ddl/ut/duplicate_col.json",
                        "ddl/ut/duplicate_col.txt",
                        1,
                        3,
                        "create table `bo_bill_detail`(\n" +
                                "`ingestionTime` timestamp,\n" +
                                "`new_user_id` string,\n" +
                                "`bill_ts` bigint,\n" +
                                "`bank_id` string,\n" +
                                "`lst_bill_amt` double,\n" +
                                "`lst_repay_amt` double,\n" +
                                "`card_limit` double,\n" +
                                "`cur_blc` double,\n" +
                                "`cur_bill_min_repay` double,\n" +
                                "`buy_cnt` double,\n" +
                                "`cur_bill_amt` double,\n" +
                                "`adj_amt` double,\n" +
                                "`rev_credit` double,\n" +
                                "`avl_amt` double,\n" +
                                "`advc_limit` double,\n" +
                                "`repay_status` string,\n" +
                                "index(key=(`new_user_id`), ts=`ingestionTime`, ttl=92160m, ttl_type=absolute)\n" +
                                ") replicanum=1, partitionnum=3 ;\n" +
                                "create table `bo_user`(\n" +
                                "`ingestionTime` timestamp,\n" +
                                "`new_user_id` string,\n" +
                                "`sex` string,\n" +
                                "`prof` string,\n" +
                                "`edu` string,\n" +
                                "`marriage` string,\n" +
                                "`hukou_typ` string,\n" +
                                "index(key=(`new_user_id`), ttl=1, ttl_type=latest)\n" +
                                ") replicanum=1, partitionnum=3 ;\n" +
                                "create table `bo_browse_history`(\n" +
                                "`ingestionTime` timestamp,\n" +
                                "`new_user_id` string,\n" +
                                "`bws_ts` bigint,\n" +
                                "`action` string,\n" +
                                "`subaction` string,\n" +
                                "index(key=(`new_user_id`), ts=`ingestionTime`, ttl=92160m, ttl_type=absolute)\n" +
                                ") replicanum=1, partitionnum=3 ;\n" +
                                "create table `action`(\n" +
                                "`reqId` string,\n" +
                                "`eventTime` timestamp,\n" +
                                "`ingestionTime` timestamp,\n" +
                                "`actionValue` int,\n" +
                                "index(key=(`reqId`), ttl=1, ttl_type=latest)\n" +
                                ") replicanum=1, partitionnum=3 ;\n" +
                                "create table `bo_detail`(\n" +
                                "`ingestionTime` timestamp,\n" +
                                "`new_user_id` string,\n" +
                                "`trx_ts` bigint,\n" +
                                "`trx_typ` string,\n" +
                                "`trx_amt` double,\n" +
                                "`is_slry` string,\n" +
                                "index(key=(`new_user_id`), ts=`ingestionTime`, ttl=92160m, ttl_type=absolute)\n" +
                                ") replicanum=1, partitionnum=3 ;\n" +
                                "create table `batch100504_flatten_request`(\n" +
                                "`reqId` string,\n" +
                                "`eventTime` timestamp,\n" +
                                "`main_id` string,\n" +
                                "`new_user_id` string,\n" +
                                "`loan_ts` bigint,\n" +
                                "`split_id` int,\n" +
                                "`time1` string,\n" +
                                "index(key=(`new_user_id`), ts=`eventTime`, ttl=92160m, ttl_type=absolute)\n" +
                                ") replicanum=1, partitionnum=3 ;\n"
                }
        };
    }

    @Test(dataProvider = "build_more_index")
    public void testDDL(String desc, String schemaPath, String sqlPath, int replicaNumber, int partitionNumber, String expct) {
        logger.info(desc);
        File file = new File(DDLEngineTest.class.getClassLoader().getResource(schemaPath).getPath());
        File sql = new File(DDLEngineTest.class.getClassLoader().getResource(sqlPath).getPath());
        try {
            String ddl = genDDL(FileUtils.readFileToString(sql, "UTF-8"), FileUtils.readFileToString(file, "UTF-8"), replicaNumber, partitionNumber);
            Assert.assertEquals(ddl, expct);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("{}", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @DataProvider(name = "ddlAndConfig")
    public Object[][] getDDLAndConfig() {
        String rootPath = "ddl";
        File root = new File(DDLEngineTest.class.getClassLoader().getResource(rootPath).getPath());
        List<File> fileList = (List<File>) FileUtils.listFiles(root, null, false);

        Map<String, File> sqlMap = new HashMap<>();
        Map<String, File> jsonMap = new HashMap<>();

        for (File file : fileList) {
            String name = file.getName();
            if (name.endsWith(".txt")) {
                sqlMap.put(name.split("\\.")[0], file);
            }
            if (name.endsWith(".json")) {
                jsonMap.put(name.split("\\.")[0], file);
            }
        }

        Object[][] params = new Object[sqlMap.size()][];
        int i = 0;
        for (String e : sqlMap.keySet()) {
            params[i++] = new Object[]{
                    e, sqlMap.get(e), jsonMap.get(e),
            };
        }
        return params;
    }

    @Test(dataProvider = "ddlAndConfig")
    public void testDDLAndConfig(String name, File ddlFile, File configFile) throws Exception {
        String rootPath = DDLEngineTest.class.getClassLoader().getResource("ddl").getPath();
        logger.info("case: {}", name);
        String ddl = genDDL(FileUtils.readFileToString(ddlFile, "UTF-8"), FileUtils.readFileToString(configFile, "UTF-8"), 1, 1);
        String config = sql2Feconfig(FileUtils.readFileToString(ddlFile, "UTF-8"), FileUtils.readFileToString(configFile, "UTF-8"));

        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser parser = new JsonParser();
        config = gson.toJson(parser.parse(config));
        String ddlPath = rootPath + "/ddl_result/" + name + ".txt";
        String configPath = rootPath + "/sql2feconfig_result/" + name + ".json";
        Assert.assertEquals(ddl, FileUtils.readFileToString(new File(ddlPath), "UTF-8"));
        Assert.assertEquals(config, FileUtils.readFileToString(new File(configPath), "UTF-8"));
    }

}
