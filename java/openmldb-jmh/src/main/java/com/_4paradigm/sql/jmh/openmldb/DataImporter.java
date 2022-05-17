package com._4paradigm.sql.jmh.openmldb;

import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Type;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.sql.BenchmarkConfig;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

public class DataImporter {
    private String dbName;
    private String tableName;
    private int pkNum;
    private int windowSize;
    private int pkBase;
    private long tsBase;
    private SqlExecutor executor;
    private List<Type.DataType> schema;
    private Set<Integer> index;
    private Set<Integer> tsIndex;


    public DataImporter() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        dbName = BenchmarkConfig.DATABASE;
        tableName = BenchmarkConfig.TABLE;
        pkNum = BenchmarkConfig.PK_NUM;
        windowSize = BenchmarkConfig.WINDOW_SIZE;
        tsBase = BenchmarkConfig.TS_BASE;
        pkBase = BenchmarkConfig.PK_BASE;
        schema = new ArrayList<>();
        index = new HashSet<>();
        tsIndex = new HashSet<>();
    }

    public boolean parseSchema() {
        NS.TableInfo tableInfo = null;
        try {
            tableInfo = executor.getTableInfo(dbName, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        Map<String, Integer> fieldMap = new HashMap<>();
        for (int idx = 0; idx < tableInfo.getColumnDescCount(); idx++) {
            schema.add(tableInfo.getColumnDesc(idx).getDataType());
            fieldMap.put(tableInfo.getColumnDesc(idx).getName(), idx);
        }
        for (int idx = 0; idx < tableInfo.getColumnKeyCount(); idx++) {
            for (int i = 0; i < tableInfo.getColumnKey(idx).getColNameCount(); i++) {
                index.add(fieldMap.get(tableInfo.getColumnKey(idx).getColName(i)));
            }
            if (tableInfo.getColumnKey(idx).hasTsName()) {
                tsIndex.add(fieldMap.get(tableInfo.getColumnKey(idx).getTsName()));
            }
        }
        return true;
    }

    public void putTableData() {
        if (!parseSchema() || schema.isEmpty()) {
            System.out.println("parse schema failed");
            return;
        }
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
                builder.append("'col").append(pos).append("'");
            } else if (type.equals(Type.DataType.kFloat)) {
                builder.append(1.3);
            } else if (type.equals(Type.DataType.kDouble)) {
                builder.append(1.4d);
            } else if (type.equals(Type.DataType.kBigInt) || type.equals(Type.DataType.kInt) ||
                    type.equals(Type.DataType.kSmallInt)) {
                builder.append(pos);
            } else if (type.equals(Type.DataType.kTimestamp)) {
                builder.append(tsBase);
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
        for (int i = 0; i < pkNum; i++) {
            String pk = "key" + String.valueOf(pkBase + i);
            for (int tsCnt = 0; tsCnt < windowSize; tsCnt++) {
                PreparedStatement state = null;
                try {
                    state = executor.getInsertPreparedStmt(dbName, insertSQL);
                    for (int idx = 0; idx < genColIndex.size(); idx++) {
                        int pos = genColIndex.get(idx);
                        Type.DataType type = schema.get(pos);
                        if (type.equals(Type.DataType.kString) || type.equals(Type.DataType.kVarchar)) {
                            state.setString(idx + 1, pk);
                        } else if (type.equals(Type.DataType.kBigInt)) {
                            if (tsIndex.contains(pos)) {
                                state.setLong(idx + 1, tsBase - tsCnt);
                            } else {
                                state.setLong(idx + 1, pkBase + i);
                            }
                        } else if (type.equals(Type.DataType.kTimestamp)) {
                            if (tsIndex.contains(pos)) {
                                state.setTimestamp(idx + 1, new Timestamp(tsBase - tsCnt));
                            } else {
                                state.setTimestamp(idx + 1, new Timestamp(tsBase + i));
                            }
                        } else if (type.equals(Type.DataType.kInt)) {
                            state.setInt(idx + 1,pkBase + i);
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
    }

    public static void main(String[] args) {
        DataImporter importer = new DataImporter();
        importer.putTableData();
    }
}
