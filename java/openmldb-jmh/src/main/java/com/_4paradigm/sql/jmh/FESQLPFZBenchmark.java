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

package com._4paradigm.sql.jmh;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.sql.tools.Relation;
import com._4paradigm.sql.tools.TableInfo;
import com._4paradigm.sql.tools.Util;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)

public class FESQLPFZBenchmark {
    private SqlExecutor executor;
    private String db;
    private int pkNum = 1;
    @Param({"500", "1000", "2000"})
    private int windowNum = 2000;
    private Map<String, TableInfo> tableMap;
    private String script;
    private String mainTable;
    List<Map<String, String>> mainTableValue;
    int pkBase = 1000000;
    private String pname = "d1" + System.nanoTime();
    public FESQLPFZBenchmark() {
        this(false);
    }
    public FESQLPFZBenchmark(boolean enableDebug ) {
        executor = BenchmarkConfig.GetSqlExecutor(enableDebug);
        db = "db" + System.nanoTime();
        tableMap = new HashMap<>();
        mainTableValue = new ArrayList<>();
    }

    public void setWindowNum(int windowNum) {
        this.windowNum = windowNum;
    }

    public void init() {
        String rawScript = Util.getContent(BenchmarkConfig.scriptUrl);
        script = rawScript.trim().replace("\n", " ");
        Relation relation = new Relation(Util.getContent(BenchmarkConfig.relationUrl));
        mainTable = relation.getMainTable();
        String ddl = Util.getContent(BenchmarkConfig.ddlUrl);
        String[] arr = ddl.split(";");
        for (String item : arr) {
            item = item.trim().replace("\n", "");
            if (item.isEmpty()) {
                continue;
            }
            TableInfo table = new TableInfo(item, relation);
            tableMap.put(table.getName(), table);
        }
        System.out.println(db);
    }

    public void putData() {
        TableInfo mainTableInfo = tableMap.get(mainTable);
        putTableData(mainTableInfo);
        for (Map.Entry<String, TableInfo> entry : tableMap.entrySet()) {
            if (entry.getKey().equals(mainTable)) {
                continue;
            }
            putTableData(entry.getValue());
        }
    }

    private void putTableData(TableInfo table) {
        boolean isMainTable = false;
        if (table.getName().equals(mainTable)) {
            isMainTable = true;
        }
        if (!isMainTable && mainTableValue.size() != pkNum) {
            return;
        }
        List<String> schema = table.getSchema();
        Set<Integer> index = table.getIndex();
        Set<Integer> tsIndex = table.getTsIndex();
        Map<Integer, String> relation = table.getColRelation();

        Map<String, String> valueMap;
        for (int i = 0; i < pkNum; i++) {
            long ts = System.currentTimeMillis();
            if (isMainTable) {
                valueMap = new HashMap<>();
            } else {
                valueMap = mainTableValue.get(i);
            }
            for (int tsCnt = 0; tsCnt < windowNum; tsCnt++) {
                StringBuilder builder = new StringBuilder();
                builder.append("insert into ");
                builder.append(table.getName());
                builder.append(" values(");
                for (int pos = 0; pos < schema.size(); pos++) {
                    if (pos > 0) {
                        builder.append(", ");
                    }
                    String type = schema.get(pos);
                    if (!isMainTable && index.contains(pos)) {
                        if (type.equals("string")) {
                            builder.append("'");
                        }
                        builder.append(valueMap.get(relation.get(pos)));
                        if (type.equals("string")) {
                            builder.append("'");
                        }
                        continue;
                    }
                    if (type.equals("string")) {
                        builder.append("'");
                        builder.append("col");
                        builder.append(pos);
                        if (index.contains(pos)) {
                            String fieldName = table.getSchemaPosName().get(pos);
                            if (!valueMap.containsKey(fieldName)) {
                                valueMap.put(fieldName, "col" + pos);
                            }
                        }
                        builder.append("'");
                    } else if (type.equals("float")) {
                        builder.append(1.3);
                    } else if (type.equals("double")) {
                        builder.append("1.4");
                    } else if (type.equals("bigint") || type.equals("timestamp") || type.equals("int")) {
                        if (index.contains(pos)) {
                            builder.append(pkBase + i);
                            String fieldName = table.getSchemaPosName().get(pos);
                            if (!valueMap.containsKey(fieldName)) {
                                valueMap.put(fieldName, String.valueOf(pkBase + i));
                            }
                        } else if (tsIndex.contains(pos)) {
                            builder.append(ts - (tsCnt + BenchmarkConfig.TIME_DIFF) * 1000);
                        } else {
                            if (type.equals("timestamp")) {
                                builder.append(ts);
                            } else {
                                builder.append(pos);
                            }
                        }
                    } else if (type.equals("bool")) {
                        builder.append(true);
                    } else if (type.equals("date")) {
                        builder.append("'2020-11-27'");
                    } else {
                        System.out.println("invalid type");
                    }
                }
                builder.append(");");
                String exeSql = builder.toString();
                //System.out.println(exeSql);
                executor.executeInsert(db, exeSql);
            }
            if (isMainTable) {
                mainTableValue.add(valueMap);
            }
        }
    }

    private PreparedStatement getPreparedStatement() throws SQLException {
        PreparedStatement requestPs = executor.getCallablePreparedStmt(db, pname);
        ResultSetMetaData metaData = requestPs.getMetaData();
        TableInfo table = tableMap.get(mainTable);
        if (table.getSchema().size() != metaData.getColumnCount()) {
            return null;
        }
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.VARCHAR) {
                requestPs.setString(i + 1, "col" + String.valueOf(i));
            } else if (columnType == Types.DOUBLE) {
                requestPs.setDouble(i + 1, 1.4d);
            } else if (columnType == Types.FLOAT) {
                requestPs.setFloat(i + 1, 1.3f);
            } else if (columnType == Types.INTEGER) {
                if (table.getIndex().contains(i)) {
                    requestPs.setInt(i + 1, pkBase + i);
                } else {
                    requestPs.setInt(i + 1, i);
                }
            } else if (columnType == Types.BIGINT) {
                if (table.getIndex().contains(i)) {
                    requestPs.setLong(i + 1, pkBase + i);
                } else {
                    requestPs.setLong(i + 1, i);
                }
            } else if (columnType == Types.TIMESTAMP) {
                requestPs.setTimestamp(i + 1, new Timestamp(System.currentTimeMillis()));
            } else if (columnType == Types.DATE) {
                requestPs.setDate(i + 1, new Date(System.currentTimeMillis()));
            }
        }
        return  requestPs;
    }

    @Setup
    public void setup() throws SQLException {
        init();
        if (!Util.executeSQL("CREATE DATABASE " + db + ";", executor)) {
            return;
        }
        Util.executeSQL("USE " + db + ";", executor);
        for (TableInfo table : tableMap.values()) {
            if (!Util.executeSQL(table.getDDL(), executor)) {
                return;
            }
        }
        String deploySQL= "DEPLOY " + pname + " " + script;
        if (!Util.executeSQL(deploySQL, executor)) {
            System.out.println(deploySQL + "execute error");
            return;
        }
        putData();
    }

    @TearDown
    public void teardown() throws SQLException {
        /*for (String name : tableMap.keySet()) {
            executor.executeDDL(db, "drop table " + name + ";");
        }
        executor.dropDB(db);*/
    }

    @Benchmark
    public void execSQL() {
        try {
            PreparedStatement ps = getPreparedStatement();
            ResultSet resultSet = ps.executeQuery();
            /*resultSet.next();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, String> val = new HashMap<>();
            int stringNum = 0;
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                System.out.println(columnName + ":" + String.valueOf(i));
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    val.put(columnName, String.valueOf(resultSet.getString(i + 1)));
                    stringNum++;
                    System.out.println(columnName + ":" + String.valueOf(i) + ":" + resultSet.getString(i+ 1));

                } else if (columnType == Types.DOUBLE) {
                    val.put(columnName, String.valueOf(resultSet.getDouble(i + 1)));
                } else if (columnType == Types.INTEGER) {
                    val.put(columnName, String.valueOf(resultSet.getInt(i + 1)));
                } else if (columnType == Types.BIGINT) {
                    val.put(columnName, String.valueOf(resultSet.getLong(i + 1)));
                } else if (columnType == Types.TIMESTAMP) {
                    val.put(columnName, String.valueOf(resultSet.getTimestamp(i + 1)));
                } else if (columnType == Types.DATE) {
                    val.put(columnName, String.valueOf(resultSet.getDate(i+ 1)));
                }
            }
            System.out.println("string num" + stringNum);
            ps.close();*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> execSQLTest() {
        try {
            PreparedStatement ps = getPreparedStatement();
            ResultSet resultSet = ps.executeQuery();
            resultSet.next();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, String> val = new HashMap<>();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i + 1);
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    val.put(columnName, String.valueOf(resultSet.getString(i + 1)));
                } else if (columnType == Types.DOUBLE) {
                    val.put(columnName, String.valueOf(resultSet.getDouble(i + 1)));
                } else if (columnType == Types.INTEGER) {
                    val.put(columnName, String.valueOf(resultSet.getInt(i + 1)));
                } else if (columnType == Types.BIGINT) {
                    val.put(columnName, String.valueOf(resultSet.getLong(i + 1)));
                } else if (columnType == Types.TIMESTAMP) {
                    val.put(columnName, String.valueOf(resultSet.getTimestamp(i + 1)));
                }
            }
            ps.close();
            return val;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws RunnerException {
      /*FESQLPFZBenchmark ben = new FESQLPFZBenchmark();
      try {
          ben.setup();
          ben.execSQL();
          ben.teardown();
      } catch (Exception e) {
          e.printStackTrace();
      }*/
        Options opt = new OptionsBuilder()
                .include(FESQLPFZBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
