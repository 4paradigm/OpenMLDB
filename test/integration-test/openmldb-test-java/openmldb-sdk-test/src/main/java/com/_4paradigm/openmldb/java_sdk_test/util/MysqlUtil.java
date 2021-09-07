package com._4paradigm.openmldb.java_sdk_test.util;


import com._4paradigm.openmldb.test_common.common.LogProxy;
import com._4paradigm.openmldb.test_common.model.DBType;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;

/**
 * @author zhaowei
 * @date 2021/3/17 2:41 PM
 */
@Slf4j
public class MysqlUtil {
    private static final Logger logger = new LogProxy(log);
    public static void createDB(String dbName){
        Properties prop = new Properties();
        try {
            prop.load(MysqlUtil.class.getClassLoader().getResourceAsStream("mysql.properties"));
            String url = prop.getProperty("url");
            String userName = prop.getProperty("user");
            String password = prop.getProperty("password");
            // jdbc:mysql://localhost:3306/test_fedb?useUnicode=true&characterEncoding=UTF-8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
            url = url.replace("test_fedb","");
            Connection conn = DriverManager.getConnection(url,userName,password);
            String sql = "CREATE DATABASE IF NOT EXISTS "+dbName+" CHARACTER SET UTF8";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.executeUpdate();
        }catch (Exception e) {
            logger.info("mysql创建数据库失败");
            e.printStackTrace();
        }
    }
    public static void createDB(){
        createDB("test_fedb");
    }
    public static boolean insertData(InputDesc inputDesc){
        List<String> insertSqls = ANSISQLUtil.getInsertSqls(inputDesc,(columnType,dataStr)->{
            if(dataStr.equals("null")){
                return dataStr;
            }
            if ((columnType.equals("string") || columnType.equals("date"))) {
                dataStr = "'" + dataStr + "'";
            }else if(columnType.equals("timestamp")){
                Timestamp timestamp = new Timestamp(Long.parseLong(dataStr));
                dataStr = timestamp.toString();
                dataStr = "'" + dataStr + "'";
            }
            return dataStr;
        });
        int count = 0;
        for(String insertSql:insertSqls){
            int n = JDBCUtil.executeUpdate(insertSql, DBType.MYSQL);
            count+=n;
        }
        return count==insertSqls.size();
    }
    public static String getCreateTableSql(InputDesc inputDesc){
        String sql = ANSISQLUtil.getCreateTableSql(inputDesc,MysqlUtil::convertColumType);
        return sql;
    }
    public static String convertColumType(String dataType){
        switch (dataType){
            case "string":
                return "varchar(100)";
            default:
                return dataType;
        }
    }
}
