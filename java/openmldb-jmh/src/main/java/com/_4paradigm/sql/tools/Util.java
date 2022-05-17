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

    public static String genDDL(String name, int indexNum) {
        int stringNum = 15;
        int doubleNum= 5;
        int timestampNum = 5;
        int bigintNum = 5;
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ").append(name).append(" (");
        builder.append("\n");
        for (int i = 0; i < stringNum; i++) {
            builder.append("col_s").append(i).append(" string,");
        }
        for (int i = 0; i < doubleNum; i++) {
            builder.append("col_d").append(i).append(" double,");
        }
        for (int i = 0; i < timestampNum; i++) {
            builder.append("col_t").append(i).append(" timestamp,");
        }
        for (int i = 0; i < bigintNum; i++) {
            builder.append("col_i").append(i).append(" bigint,");
        }
        for (int i = 0; i < indexNum; i++) {
            builder.append("index(key = ").append("col_s").append(i).append( ", ttl=0m, ttl_type=absolute, ts = col_t0),");
        }
        builder.delete(builder.length() - 1, builder.length() - 1);
        builder.append(") OPTIONS (REPLICANUM = 1);");
        return builder.toString();
    }

    public static String genScript(int windowNum, int windowSize, int unionNum, int joinNum) {
        StringBuilder builder = new StringBuilder();
        if (joinNum > 0) {
            builder.append("SELECT * FROM \n");
            builder.append("(");
        }
        builder.append("SELECT \n");
        builder.append("col_s0,\n");
        builder.append("concat(col_s1, col_d0) as concat_col_s1,\n");
        builder.append("upper(col_s2) as upper_col_s2,\n");
        builder.append("substr(col_s3, 3) as substr_col_s3,\n");
        builder.append("year(col_t0) as year_col_t0,\n");
        builder.append("string(col_i2) as str_col_i2,\n");
        builder.append("add(col_i1, col_i3) as add_i1_i3,\n");
        for (int i = 0; i < windowNum; i++) {
            builder.append("distinct_count(col_s1) OVER w").append(i).append(" AS distinct_count_w").append(i).append("_col_s1,\n");
            builder.append("sum(col_i1) OVER w").append(i).append(" AS sum_w").append(i).append("_col_i1,\n");
            builder.append("count(col_s11) OVER w").append(i).append(" AS count_w").append(i).append("_col_s11,\n");
            builder.append("avg(col_i4) OVER w").append(i).append(" AS avg_w").append(i).append("_col_i4,\n");
            builder.append("case when !isnull(at(col_s5, 0)) OVER w").append(i).append(" then count(col_s5) OVER w").append(i)
                    .append(" else null end AS case_when_count_w").append(i).append("_col_s5,\n");
            builder.append("case when !isnull(at(col_i3, 0)) OVER w").append(i).append(" then count(col_i3) OVER w").append(i)
                    .append(" else null end AS case_when_count_w").append(i).append("_col_i3,\n");
        }
        builder.append(" from mt\n");
        builder.append("window ");
        for (int i = 0; i < windowNum; i++) {
            builder.append("w").append(i).append(" as (");
            if (unionNum > 0) {
                builder.append("UNION ut").append(i);
                unionNum--;
            }
            builder.append(" partition by ").append("col_s").append(i)
                    .append(" order by col_t0 rows_range between 30d PRECEDING AND CURRENT ROW MAXSIZE ").append(windowSize).append("),\n");
        }
        builder.delete(builder.length() - 2, builder.length() - 1);
        if (joinNum > 0) {
            builder.append(") as out0");
            for (int i = 0; i < joinNum; i++) {
                String curTable = "lt" + String.valueOf(i);
                String table = "out" + String.valueOf(i + 1);
                builder.append(" LAST JOIN\n");
                builder.append("(SELECT \n");
                builder.append(curTable).append(".col_s0 as ").append(table).append("_col_s0,\n");
                builder.append("concat(").append(curTable).append(".col_s1, mt.col_d0) as ")
                        .append(table).append("_concat_col_s1,\n");
                builder.append("upper(mt.col_s2) as ").append(table).append("_upper_col_s2,\n");
                builder.append("substr(").append(curTable).append(".col_s3, 3) as ")
                        .append(table).append("_substr_col_s3,\n");
                builder.append("year(mt.col_t0) as ").append(table).append("_year_col_t0,\n");
                builder.append("string(").append(curTable).append(".col_i2) as ").append(table).append("_str_col_i2,\n");
                builder.append("add(").append(curTable).append(".col_i1, mt.col_i3) as ")
                        .append(table).append("_add_i1_i3\n");
                builder.append("from mt LAST JOIN ").append(curTable).append(" order by ")
                        .append(curTable).append(".col_t0 ON mt.col_s0 = ")
                        .append(curTable).append(".col_s0 ").append("\n");
                builder.append(") as ").append(table)
                        .append( " ON out0.col_s0 = ").append(table).append(".").append(table).append("_col_s0");
            }
        }
        builder.append(";");
        return builder.toString();
    }

    public static boolean loadData(TableSchema tableSchema, int windowSize, SqlExecutor executor) {
        String dbName = tableSchema.getDataBase();
        String tableName = tableSchema.getTableName();
        List<Type.DataType> schema = tableSchema.getSchema();
        Set<Integer> index = tableSchema.getIndex();
        Set<Integer> tsIndex = tableSchema.getTsIndex();

        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(tableName).append(" values(");
        List<Integer> genColIndex = new ArrayList<>();
        for (int pos = 0; pos < schema.size(); pos++) {
            if (pos > 0) {
                builder.append(", ");
            }
            if (index.contains(pos) || tsIndex.contains(pos)) {
                builder.append("?");
                genColIndex.add(pos);
                continue;
            }
            Type.DataType type = schema.get(pos);
            if (type.equals(Type.DataType.kString) || type.equals(Type.DataType.kVarchar)) {
                builder.append("'val").append(BenchmarkConfig.PK_BASE).append("'");
            } else if (type.equals(Type.DataType.kFloat)) {
                builder.append(1.3);
            } else if (type.equals(Type.DataType.kDouble)) {
                builder.append(1.4d);
            } else if (type.equals(Type.DataType.kBigInt) || type.equals(Type.DataType.kInt) ||
                    type.equals(Type.DataType.kSmallInt)) {
                builder.append(pos);
            } else if (type.equals(Type.DataType.kTimestamp)) {
                builder.append(BenchmarkConfig.TS_BASE);
            } else if (type.equals(Type.DataType.kBool)) {
                builder.append(true);
            } else if (type.equals(Type.DataType.kDate)) {
                builder.append("'2022-05-11'");
            } else {
                System.out.println("invalid type");
            }

        }
        builder.append(");");
        String insertSQL = builder.toString();
        for (int i = 0; i < BenchmarkConfig.PK_NUM; i++) {
            for (int tsCnt = 0; tsCnt < windowSize; tsCnt++) {
                PreparedStatement state = null;
                try {
                    state = executor.getInsertPreparedStmt(dbName, insertSQL);
                    for (int idx = 0; idx < genColIndex.size(); idx++) {
                        int pos = genColIndex.get(idx);
                        Type.DataType type = schema.get(pos);
                        if (type.equals(Type.DataType.kString) || type.equals(Type.DataType.kVarchar)) {
                            state.setString(idx + 1, "k" + String.valueOf(10 + idx) + String.valueOf(BenchmarkConfig.PK_BASE + i));
                        } else if (type.equals(Type.DataType.kBigInt)) {
                            if (tsIndex.contains(pos)) {
                                state.setLong(idx + 1, BenchmarkConfig.TS_BASE - tsCnt);
                            } else {
                                state.setLong(idx + 1, BenchmarkConfig.PK_BASE + i);
                            }
                        } else if (type.equals(Type.DataType.kTimestamp)) {
                            if (tsIndex.contains(pos)) {
                                state.setTimestamp(idx + 1, new Timestamp(BenchmarkConfig.TS_BASE - tsCnt));
                            } else {
                                state.setTimestamp(idx + 1, new Timestamp(BenchmarkConfig.TS_BASE + i));
                            }
                        } else if (type.equals(Type.DataType.kInt)) {
                            state.setInt(idx + 1, BenchmarkConfig.PK_BASE + i);
                        } else {
                            System.out.println("invalid type");
                        }
                    }
                    state.execute();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (state != null) {
                        try {
                            state.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        return true;
    }

    public static PreparedStatement getPreparedStatement(String deployName, int numberKey,
                                                         TableSchema tableSchema,
                                                         SqlExecutor executor) throws SQLException {

        String dbName = tableSchema.getDataBase();
        List<Type.DataType> schema = tableSchema.getSchema();
        Set<Integer> index = tableSchema.getIndex();
        Set<Integer> tsIndex = tableSchema.getTsIndex();
        PreparedStatement requestPs = executor.getCallablePreparedStmt(dbName, deployName);
        ResultSetMetaData metaData = requestPs.getMetaData();
        if (schema.size() != metaData.getColumnCount()) {
            return null;
        }
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.VARCHAR) {
                if (index.contains(i)) {
                    requestPs.setString(i + 1, "k" + String.valueOf(10 + i) + String.valueOf(numberKey));
                } else {
                    requestPs.setString(i + 1, "val" + String.valueOf(numberKey));
                }
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, 1.4d);
            } else if (columnType == Types.FLOAT) {
                requestPs.setFloat(i + 1, 1.3f);
            } else if (columnType == Types.INTEGER) {
                if (index.contains(i)) {
                    requestPs.setInt(i + 1, numberKey);
                } else {
                    requestPs.setInt(i + 1, i);
                }
            } else if (columnType == Types.BIGINT) {
                if (index.contains(i)) {
                    requestPs.setLong(i + 1, numberKey);
                } else if (tsIndex.contains(i)) {
                    requestPs.setLong(i + 1, System.currentTimeMillis());
                } else {
                    requestPs.setLong(i + 1, i);
                }
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(System.currentTimeMillis()));
            } else if (columnType == Types.DATE) {
                requestPs.setDate(i + 1, new Date(System.currentTimeMillis()));
            } else if (columnType == Types.BOOLEAN) {
                requestPs.setBoolean(i + 1, true);
            }
        }
        return  requestPs;
    }

}
