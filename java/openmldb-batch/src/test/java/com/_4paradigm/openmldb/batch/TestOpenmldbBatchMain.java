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

package com._4paradigm.openmldb.batch;

import com._4paradigm.openmldb.batch.utils.SparkConfig;
import com._4paradigm.openmldb.batch.utils.SqlUtils;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;


public class TestOpenmldbBatchMain {
    private static final Logger logger = LoggerFactory.getLogger(TestOpenmldbBatchMain.class);
    protected static JsonParser jsonParser = new JsonParser();
    protected static Gson gson = new Gson();

    @DataProvider(name = "skew_data")
    public Object[][] getConfig() {
        // 检查数据倾斜和非数据倾斜模式下的性能结果
        // 相同的脚本，不同的配置，输出路径不同，对比字段名，不同的spark配置
        return new Object[][] {
                new Object[] {
                        "风电场景 条数窗口",
                        "fz/fengdian/script.sql",
                        "fz/fengdian/spark.json",
                        new String[]{"reqId", "flattenRequest_col_131_window_avg_829", "flattenRequest_col_161_window_avg_830", "flattenRequest_col_74_window_avg_831"},
                        "output/fengdian/skew",
                        "output/fengdian/no-skew",
                        "spark.master=local spark.openmldb.skew.watershed=1 spark.openmldb.test.print=false spark.openmldb.test.print.sampleInterval=1 spark.openmldb.test.print.printContent=false spark.openmldb.mode=skew spark.openmldb.skew.level=2 spark.openmldb.groupby.partitions=20 spark.sql.shuffle.partitions=8",
                        "spark.openmldb.mode=normal openmldb.master=local"
                },
                new Object[] {
                        "风电场景 时间窗口",
                        "fz/fengdian/time.sql",
                        "fz/fengdian/spark.json",
                        new String[]{"reqId", "flattenRequest_col_131_window_avg_829", "flattenRequest_col_161_window_avg_830", "flattenRequest_col_74_window_avg_831"},
                        "output/fengdian/skew",
                        "output/fengdian/no-skew",
                        "spark.master=local spark.openmldb.skew.watershed=1 spark.openmldb.test.print=false spark.openmldb.mode=skew spark.openmldb.skew.level=2 spark.openmldb.groupby.partitions=20 spark.sql.shuffle.partitions=8",
                        "spark.openmldb.mode=normal openmldb.master=local"
                },
                new Object[] {
                        "风电场景 时间和条数窗口混合",
                        "fz/fengdian/time_cnt.sql",
                        "fz/fengdian/spark.json",
                        new String[]{"reqId", "flattenRequest_col_131_window_avg_829", "flattenRequest_col_161_window_avg_830", "flattenRequest_col_74_window_avg_831"},
                        "output/fengdian/skew",
                        "output/fengdian/no-skew",
                        "spark.master=local spark.openmldb.skew.watershed=1 spark.openmldb.test.print=false spark.openmldb.mode=skew spark.openmldb.skew.level=2 spark.openmldb.groupby.partitions=20 spark.sql.shuffle.partitions=8",
                        "spark.openmldb.mode=normal openmldb.master=local"
                }
        };
    }

    public static SparkConfig toSparkConf(String jsonPath, String scriptPath, String output, String sparkConfig) {
        SparkConfig config = SqlUtils.parseFeconfigJsonPath(jsonPath);
        File scriptFile = new File(scriptPath);
        String sqlScript = null;
        try {
            sqlScript = FileUtils.readFileToString(scriptFile, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        config.setSql(sqlScript);
        config.setOutputPath(output);
        config.setSparkConfig(Arrays.asList(sparkConfig.split(" ")));
        return config;
    }

    /* TODO: This case is too heavy for unit test, uncomment after modification
    @Test(dataProvider = "skew_data")
    public void testSkewModeResult(String desc, String scriptPath, String jsonPath, String[] checkField, String output1, String output2, String config1, String config2) throws IOException {
        logger.info(desc);
        jsonPath = TestOpenmldbMain.class.getClassLoader().getResource(jsonPath).getPath();
        scriptPath = TestOpenmldbMain.class.getClassLoader().getResource(scriptPath).getPath();
        String root = TestOpenmldbMain.class.getClassLoader().getResource(".").getPath();
        FileUtils.forceMkdir(new File(root + "/" + output1));
        FileUtils.forceMkdir(new File(root + "/" + output2));
        output1 = TestOpenmldbMain.class.getClassLoader().getResource(output1).getPath();
        output2 = TestOpenmldbMain.class.getClassLoader().getResource(output2).getPath();

        SparkConfig sc1 = toSparkConf(jsonPath, scriptPath, output1, config1);
        SparkConfig sc2 = toSparkConf(jsonPath, scriptPath, output2, config2);
        TestOpenmldbMain.run(sc1);
        TestOpenmldbMain.run(sc2);

        SparkSession sess = SparkSession.builder()
                .appName("test").master("local")
                .getOrCreate();

        output1 = output1 + "/data";
        output2 = output2 + "/data";
        sess.read().parquet(output1).createOrReplaceTempView("t1");
        sess.read().parquet(output2).createOrReplaceTempView("t2");
        List<String> cons = new ArrayList<>();
        for (String e : checkField) {
            cons.add(String.format("t1.%s = t2.%s", e, e));
        }

        String script = "select t1." + checkField[0] + " from t1 left join t2  on " + StringUtils.join(cons, " and ") + " where t2." + checkField[0] + " is null ;";
        logger.info(script);
        val df = sess.sqlContext().sql(script);
        df.show();
        Assert.assertEquals(0, df.count());

        df.createOrReplaceTempView("t3");
        script = String.format("select * from t1 left join t2  on t1.%s = t2.%s right join t3 on t1.%s = t3.%s;", checkField[0], checkField[0], checkField[0], checkField[0]);
        logger.info(script);
        val res = sess.sqlContext().sql(script);
        res.show();
        Assert.assertEquals(0, res.count());
        sess.close();

    }
    */

}
