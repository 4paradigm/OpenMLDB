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
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2021/2/5 5:23 PM
 */
@Slf4j
public class DiffResultChecker extends BaseChecker{

    // private FesqlResult sqlite3Result;
    public DiffResultChecker(OpenMLDBResult openMLDBResult, Map<String, OpenMLDBResult> resultMap){
        super(openMLDBResult,resultMap);
        // sqlite3Result = resultMap.get("sqlite3");
    }

    @Override
    public void check() throws Exception {
        for(String key:resultMap.keySet()){
            if(key.equals(SQLCaseType.kSQLITE3.getTypeName())){
                checkSqlite3(resultMap.get(key));
            }else{
                checkMysql(resultMap.get(key));
            }
        }
    }
    public void checkMysql(OpenMLDBResult mysqlResult) throws Exception {
        log.info("diff mysql check");
        //验证success
        boolean fesqlOk = openMLDBResult.isOk();
        boolean sqlite3Ok = mysqlResult.isOk();
        Assert.assertEquals(fesqlOk,sqlite3Ok,"success 不一致，fesql："+fesqlOk+",sqlite3："+sqlite3Ok);
        if(!fesqlOk) return;
        //验证result
        List<List<Object>> fesqlRows = openMLDBResult.getResult();
        List<List<Object>> mysqlRows = mysqlResult.getResult();
        log.info("fesqlRows:{}", fesqlRows);
        log.info("mysqlRows:{}", mysqlRows);
        // Assert.assertEquals(fesqlRows.size(), mysqlRows.size(),
        //         String.format("ResultChecker fail: mysql size %d, fesql size %d", mysqlRows.size(), fesqlRows.size()));
        Assert.assertEquals(fesqlRows,mysqlRows,String.format("ResultChecker fail: mysql: %s, fesql: %s", mysqlRows, fesqlRows));
    }
    public void checkSqlite3(OpenMLDBResult sqlite3Result) throws Exception {
        log.info("diff sqlite3 check");
        //验证success
        boolean fesqlOk = openMLDBResult.isOk();
        boolean sqlite3Ok = sqlite3Result.isOk();
        Assert.assertEquals(fesqlOk,sqlite3Ok,"success 不一致，fesql："+fesqlOk+",sqlite3："+sqlite3Ok);
        if(!fesqlOk) return;
        //验证result
        List<List<Object>> fesqlRows = openMLDBResult.getResult();
        List<List<Object>> sqlite3Rows = sqlite3Result.getResult();
        log.info("fesqlRows:{}", fesqlRows);
        log.info("sqlite3Rows:{}", sqlite3Rows);
        Assert.assertEquals(fesqlRows.size(), sqlite3Rows.size(),
                String.format("ResultChecker fail: sqlite3 size %d, fesql size %d", sqlite3Rows.size(), fesqlRows.size()));
        for (int i = 0; i < fesqlRows.size(); ++i) {
            List<Object> actual_list = fesqlRows.get(i);
            List<Object> expect_list = sqlite3Rows.get(i);
            Assert.assertEquals(actual_list.size(), expect_list.size(), String.format(
                    "ResultChecker fail at %dth row: sqlite3 row size %d, fesql row size %d",
                    i, expect_list.size(), actual_list.size()));
            for (int j = 0; j < actual_list.size(); ++j) {
                Object fesql_val = actual_list.get(j);
                Object sqlite3_val = expect_list.get(j);

                if(String.valueOf(fesql_val).equals("NaN")){
                    fesql_val = null;
                }

                if(fesql_val != null && fesql_val instanceof Boolean){
                    sqlite3_val = sqlite3_val.equals(0)?false:true;
                    Assert.assertEquals(String.valueOf(fesql_val), String.valueOf(sqlite3_val), String.format(
                            "ResultChecker fail: row=%d column=%d sqlite3=%s fesql=%s\nsqlite3 %s\nfesql %s",
                            i, j, sqlite3_val, fesql_val,
                            sqlite3Result.toString(),
                            openMLDBResult.toString()));
                }else if (sqlite3_val != null && sqlite3_val instanceof Double) {
                    // Assert.assertTrue(expect_val != null && expect_val instanceof Double);
                    if(fesql_val instanceof Float){
                        fesql_val = ((Float)fesql_val).doubleValue();
                    }else if(fesql_val instanceof Timestamp){
                        fesql_val = (double)((Timestamp)fesql_val).getTime();
                    }else if(fesql_val instanceof String){
                        fesql_val = Double.parseDouble((String)fesql_val);
                    }
                    Assert.assertEquals(
                            (Double) fesql_val, (Double) sqlite3_val, 1e-4,
                            String.format("ResultChecker fail: row=%d column=%d sqlite3=%s fesql=%s\nsqlite3 %s\nfesql %s",
                                    i, j, sqlite3_val, fesql_val,
                                    sqlite3Result.toString(),
                                    openMLDBResult.toString())
                    );

                } else if(fesql_val != null && fesql_val instanceof Timestamp){
                    fesql_val = ((Timestamp)fesql_val).getTime();
                    Assert.assertEquals(String.valueOf(fesql_val), String.valueOf(sqlite3_val), String.format(
                            "ResultChecker fail: row=%d column=%d sqlite3=%s fesql=%s\nsqlite3 %s\nfesql %s",
                            i, j, sqlite3_val, fesql_val,
                            sqlite3Result.toString(),
                            openMLDBResult.toString()));
                } else{
                    Assert.assertEquals(String.valueOf(fesql_val), String.valueOf(sqlite3_val), String.format(
                            "ResultChecker fail: row=%d column=%d sqlite3=%s fesql=%s\nsqlite3 %s\nfesql %s",
                            i, j, sqlite3_val, fesql_val,
                            sqlite3Result.toString(),
                            openMLDBResult.toString()));
                }
            }
        }
    }
}
