package com._4paradigm.openmldb.java_sdk_test.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.util.Properties;

/**
 * @author zhaowei
 * @date 2021/3/8 6:21 PM
 */
public class Sqlite3Conn {
    private ComboPooledDataSource cpds;
    private Sqlite3Conn() {
        cpds=new ComboPooledDataSource();
        //加载配置文件
        Properties props = new Properties();
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("sqlite3.properties"));
            cpds.setDriverClass(props.getProperty("driver"));
            cpds.setJdbcUrl(props.getProperty("url"));
            cpds.setUser(props.getProperty("user"));
            cpds.setPassword(props.getProperty("password"));

            cpds.setMaxPoolSize(Integer.parseInt(props.getProperty("MaxPoolSize")));
            cpds.setMinPoolSize(Integer.parseInt(props.getProperty("MinPoolSize")));
            cpds.setInitialPoolSize(Integer.parseInt(props.getProperty("InitialPoolSize")));
            cpds.setMaxStatements(Integer.parseInt(props.getProperty("MaxStatements")));
            cpds.setMaxIdleTime(Integer.parseInt(props.getProperty("MaxIdleTime")));

        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    private static class ClassHolder {
        private static final Sqlite3Conn INSTANCE = new Sqlite3Conn();
    }

    public static Sqlite3Conn of() {
        return ClassHolder.INSTANCE;
    }

    public ComboPooledDataSource getDataSource(){
        return cpds;
    }


    public Connection getConnection(){
        Connection conn = null;
        try {
            conn = cpds.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) {
        Connection connection = Sqlite3Conn.of().getConnection();
        System.out.println("已经连接成功");
        System.out.println(connection);
    }

}

