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

package com._4paradigm.sql.tools;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import com._4paradigm.openmldb.proto.Type;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.sql.BenchmarkConfig;

public class Util {
    public static String getContent(String httpUrl) {
        try {
            URL url = new URL(httpUrl);
            HttpURLConnection con = null;
            if (BenchmarkConfig.NeedProxy()) {
                con = (HttpURLConnection) url.openConnection(new Proxy(Proxy.Type.SOCKS,
                        new InetSocketAddress("127.0.0.1",1080)));
            } else {
                con = (HttpURLConnection) url.openConnection();
            }
            con.setRequestMethod("GET");
            con.connect();
            if (con.getResponseCode() == 200) {
                InputStream is = con.getInputStream();
                StringBuilder builder = new StringBuilder();
                int len = 0;
                byte[] buffer = new byte[1024];
                while ((len = is.read(buffer)) != -1) {
                    byte[] temp = new byte[len];
                    System.arraycopy(buffer, 0 , temp, 0, len);
                    builder.append(new String(temp, "utf-8"));
                }
                return builder.toString();
            } else {
                System.out.println("request failed");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static Map<String, TableInfo> parseDDL(String ddlUrl, Relation relation) {
        String ddl = Util.getContent(ddlUrl);
        String[] arr = ddl.split(";");
        Map<String, TableInfo> tableMap = new HashMap<>();
        for (String item : arr) {
            item = item.trim().replace("\n", "");
            if (item.isEmpty()) {
                continue;
            }
            TableInfo table = new TableInfo(item, relation);
            tableMap.put(table.getName(), table);
        }
        return tableMap;
    }

    public static boolean executeSQL(String sql, SqlExecutor executor) {
        java.sql.Statement state = executor.getStatement();
        try {
            boolean ret = state.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static Map<String, String> extractResultSet(ResultSet resultSet) {
        Map<String, String> val = new HashMap<>();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    val.put(columnName, String.valueOf(resultSet.getString(i + 1)));
                } else if (columnType == Types.DOUBLE) {
                    val.put(columnName, String.valueOf(resultSet.getDouble(i + 1)));
                } else if (columnType == Types.FLOAT) {
                    val.put(columnName, String.valueOf(resultSet.getFloat(i + 1)));
                } else if (columnType == Types.INTEGER) {
                    val.put(columnName, String.valueOf(resultSet.getInt(i + 1)));
                } else if (columnType == Types.BIGINT) {
                    val.put(columnName, String.valueOf(resultSet.getLong(i + 1)));
                } else if (columnType == Types.TIMESTAMP) {
                    val.put(columnName, String.valueOf(resultSet.getTimestamp(i + 1)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return val;
    }

}
