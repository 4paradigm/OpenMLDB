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
