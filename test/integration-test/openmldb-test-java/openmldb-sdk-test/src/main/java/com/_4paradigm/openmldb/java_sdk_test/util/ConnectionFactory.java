package com._4paradigm.openmldb.java_sdk_test.util;




import com._4paradigm.openmldb.test_common.model.DBType;

import java.sql.Connection;

/**
 * @author zhaowei
 * @date 2021/3/17 11:48 AM
 */
public class ConnectionFactory {
    private C3p0 sqlite3;
    private C3p0 mysql;
    private ConnectionFactory() {
        sqlite3 = new C3p0("sqlite3.properties");
        mysql = new C3p0("mysql.properties");
        MysqlUtil.createDB();
    }

    private static class ClassHolder {
        private static final ConnectionFactory INSTANCE = new ConnectionFactory();
    }

    public static ConnectionFactory of() {
        return ClassHolder.INSTANCE;
    }

    public Connection getSqlite3Conn(){
        return sqlite3.getConnection();
    }

    public Connection getMysqlConn(){
        return mysql.getConnection();
    }

    public Connection getConn(DBType dbType){
        switch (dbType){
            case SQLITE3:
                return getSqlite3Conn();
            case MYSQL:
                return getMysqlConn();
            default:
                throw new RuntimeException("unsupport db type");
        }
    }

    public static void main(String[] args) {
        Connection conn = ConnectionFactory.of().getConn(DBType.MYSQL);
        System.out.println(conn);
        // MysqlUtil.createDB("test_fesql");
    }
}
