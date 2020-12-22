package com._4paradigm.fesql.common;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
                        "几千列的场景",
                        "ddl/4k.json",
                        "ddl/4k.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "????op???",
                        "ddl/failed/all_op.json",
                        "ddl/failed/all_op.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "?????????",
                        "ddl/multi_table_cnt_window.json",
                        "ddl/multi_table_cnt_window.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "????????",
                        "ddl/multi_table_max_cnt_window.json",
                        "ddl/multi_table_max_cnt_window_more.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "ddl/multi_table_max_cnt_window.json",
                        "ddl/multi_table_max_cnt_window.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "ddl/multi_table_min_cnt_window.json",
                        "ddl/multi_table_min_cnt_window_more.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "ddl/multi_table_min_cnt_window.json",
                        "ddl/multi_table_min_cnt_window.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "ddl/multi_table_multi_relation.json",
                        "ddl/multi_table_multi_relation.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "ddl/rong_e.json",
                        "ddl/rong_e.txt",
                        1,
                        1,
                        ""
                }
                //   new Object[] {
                //           "multi col",
                //           "ddl/",
                //           "ddl/",
                //           1,
                //           1
                //   },

        };
    }


    @DataProvider(name = "performance_script")
    public Object[][] getPerformanceSqlScript() {
        return new Object[][] {
                new Object[] {
                        "列数多",
                        "performance/4k.json",
                        "performance/4k.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "????op???",
                        "performance/all_op.json",
                        "performance/all_op.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "?????????",
                        "performance/batch_request100680.json",
                        "performance/batch_request100680.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "????????",
                        "performance/constant_column.json",
                        "performance/constant_column.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "performance/gg_studio.json",
                        "performance/gg_studio.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "performance/only_one.json",
                        "performance/only_one.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "performance/rong_e.json",
                        "performance/rong_e.txt",
                        1,
                        1,
                        ""
                },
                new Object[] {
                        "multi col",
                        "performance/timestamp2date.json",
                        "performance/timestamp2date.txt",
                        1,
                        1,
                        ""
                },
               new Object[] {
                       "luoji场景",
                       "fz/luoji.json",
                       "fz/luoji.txt",
                       1,
                       1,
                        ""
               },
                new Object[] {
                        "see_click场景",
                        "fz/see_click.json",
                        "fz/see_click.txt",
                        1,
                        1,
                        ""
                }

        };
    }

    public static void writeFile(File file, String content) throws IOException {
        FileUtils.write(file, content, "UTF-8");
    }

    @Test(dataProvider = "build_more_index")
    public void testDDL(String desc, String schemaPath, String sqlPath, int replicaNumber, int partitionNumber, String expct) {
        logger.info(desc);
        File file = new File(DDLEngineTest.class.getClassLoader().getResource(schemaPath).getPath());
        File sql = new File(DDLEngineTest.class.getClassLoader().getResource(sqlPath).getPath());
//        File output = new File()
        try {
            String ddl = genDDL(FileUtils.readFileToString(sql, "UTF-8"), FileUtils.readFileToString(file, "UTF-8"), replicaNumber, partitionNumber);
            System.out.printf(ddl);
            File output = new File(DDLEngineTest.class.getClassLoader().getResource(sqlPath).getPath() + ".ddl.txt");
            FileUtils.touch(output);
            FileUtils.write(output, ddl, "UTF-8");
//            getTableDefs(FileUtils.readFileToString(file, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAudoDDL() throws Exception {

        String rootPath = "ddl";
        File root = new File(DDLEngineTest.class.getClassLoader().getResource(rootPath).getPath());

//        File[] cases = root.listFiles();
        Map<String, File> sqlMap = new HashMap<>();
        Map<String, File> jsonMap = new HashMap<>();
        List<File> fileList = (List<File>)FileUtils.listFiles(root, null, false);
        for (File e : fileList) {
            String nane = e.getName();
            System.out.println(e.getName());
            if (nane.endsWith(".txt")) {
                sqlMap.put(nane.split("\\.")[0], e);
            }
            if (nane.endsWith(".json")) {
                jsonMap.put(nane.split("\\.")[0], e);
            }
        }
        for (String e : sqlMap.keySet()) {
//            String e = k.split("\.")split

            String ddl = genDDL(FileUtils.readFileToString(sqlMap.get(e), "UTF-8"), FileUtils.readFileToString(jsonMap.get(e), "UTF-8"), 1, 1);
            String config = sql2Feconfig(FileUtils.readFileToString(sqlMap.get(e), "UTF-8"), FileUtils.readFileToString(jsonMap.get(e), "UTF-8"));


            rootPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/";

            String ddlPath = rootPath + "/ddl/ddl_result/" + e + ".txt";
            String configPath = rootPath + "/ddl/sql2feconfig_result/" + e + ".json";

            File temp = new File("/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/ddl_result");
            temp.createNewFile();
//            FileUtils.forceMkdirParent(temp);
            temp  = new File("/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/sql2feconfig");
            temp.createNewFile();

//            FileUtils.forceMkdirParent(()));


            File file = new File(ddlPath);
            FileWriter writer = null;
            String context = ddl;
            try {
                writer = new FileWriter(file, false);
                BufferedWriter bufferedWriter = new BufferedWriter(writer);
                bufferedWriter.write(context);
                bufferedWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            file = new File(configPath);
            context = config;
            try {
                writer = new FileWriter(file, false);
                BufferedWriter bufferedWriter = new BufferedWriter(writer);
                bufferedWriter.write(context);
                bufferedWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }

    @Test(dataProvider = "performance_script")
    public void testOutputSchema(String desc, String schemaPath, String sqlPath, int replicaNumber, int partitionNumber, String expct) {
        logger.info(desc);
        File file = new File(DDLEngineTest.class.getClassLoader().getResource(schemaPath).getPath());
        File sql = new File(DDLEngineTest.class.getClassLoader().getResource(sqlPath).getPath());
//        File output = new File()
        try {
            String ddl = sql2Feconfig(FileUtils.readFileToString(sql, "UTF-8"), FileUtils.readFileToString(file, "UTF-8"));
            System.out.printf(ddl);
            File output = new File(DDLEngineTest.class.getClassLoader().getResource(sqlPath).getPath() + ".ddl.txt");
            FileUtils.touch(output);
            FileUtils.write(output, ddl, "UTF-8");
//            getTableDefs(FileUtils.readFileToString(file, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }
}
