package com._4paradigm.fesql.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com._4paradigm.fesql.common.DDLEngine.genDDL;
import static com._4paradigm.fesql.common.DDLEngine.sql2Feconfig;

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

    @Test
    public void testDDLAndConfig() throws Exception {
        String rootPath = "ddl";
        File root = new File(DDLEngineTest.class.getClassLoader().getResource(rootPath).getPath());
        rootPath = DDLEngineTest.class.getClassLoader().getResource(rootPath).getPath();
        Map<String, File> sqlMap = new HashMap<>();
        Map<String, File> jsonMap = new HashMap<>();
        List<File> fileList = (List<File>)FileUtils.listFiles(root, null, false);
        for (File e : fileList) {
            String name = e.getName();
            if (name.endsWith(".txt")) {
                sqlMap.put(name.split("\\.")[0], e);
            }
            if (name.endsWith(".json")) {
                jsonMap.put(name.split("\\.")[0], e);
            }
        }
        for (String e : sqlMap.keySet()) {
            logger.info("case: {}", e);
            String ddl = genDDL(FileUtils.readFileToString(sqlMap.get(e), "UTF-8"), FileUtils.readFileToString(jsonMap.get(e), "UTF-8"), 1, 1);
            String config = sql2Feconfig(FileUtils.readFileToString(sqlMap.get(e), "UTF-8"), FileUtils.readFileToString(jsonMap.get(e), "UTF-8"));

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            JsonParser parser = new JsonParser();
            config = gson.toJson(parser.parse(config));
            String ddlPath = rootPath + "/ddl_result/" + e + ".txt";
            String configPath = rootPath + "/sql2feconfig_result/" + e + ".json";
            Assert.assertEquals(ddl, FileUtils.readFileToString(new File(ddlPath), "UTF-8"));
            Assert.assertEquals(config, FileUtils.readFileToString(new File(configPath), "UTF-8"));
        }
    }

}
