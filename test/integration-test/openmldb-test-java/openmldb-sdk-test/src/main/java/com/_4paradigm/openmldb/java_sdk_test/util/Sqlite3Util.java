package com._4paradigm.openmldb.java_sdk_test.util;

import com._4paradigm.openmldb.test_common.model.DBType;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/8 6:23 PM
 */
@Slf4j
public class Sqlite3Util {

    public static boolean insertData(InputDesc inputDesc){
        List<String> insertSqls = ANSISQLUtil.getInsertSqls(inputDesc,(columnType,dataStr)->{
            if (!dataStr.equals("null") && (columnType.equals("string") || columnType.equals("date"))) {
                dataStr = "'" + dataStr + "'";
            }
            if(dataStr.equals("true")){
                dataStr = "1";
            }else if(dataStr.equals("false")){
                dataStr = "0";
            }
            return dataStr;
        });
        int count = 0;
        for(String insertSql:insertSqls){
            int n = JDBCUtil.executeUpdate(insertSql, DBType.SQLITE3);
            count+=n;
        }
        return count==insertSqls.size();
    }

    public static String getCreateTableSql(InputDesc inputDesc){
        String sql = ANSISQLUtil.getCreateTableSql(inputDesc,null);
        return sql;
    }

}
