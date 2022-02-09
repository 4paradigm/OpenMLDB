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

package com._4paradigm.sql.stability;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.openmldb.sdk.QueryFuture;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.sql.tools.Relation;
import com._4paradigm.sql.tools.TableInfo;
import com._4paradigm.sql.tools.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FEQLFZPerf {
    private static Logger logger = LoggerFactory.getLogger(FEQLFZPerf.class);
    private SqlExecutor executor;
    private String db;
    private String pName;
    private String script;
    private String mainTable;
    private Map<String, TableInfo> tableMap;
    private Map<String, String> tableInsertSqlMap = new HashMap<>();
    private List<Integer> commonColumnIndices;

    private ExecutorService putExecuteService;
    private ExecutorService queryExecuteService;
    private Random random = new Random();
    private AtomicBoolean running = new AtomicBoolean(true);

    public FEQLFZPerf() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        commonColumnIndices = new ArrayList<>();
        init();
    }

    public void init() {
        String rawScript = Util.getContent(BenchmarkConfig.scriptUrl);
        script = rawScript.trim().replace("\n", " ");
        Relation relation = new Relation(Util.getContent(BenchmarkConfig.relationUrl));
        mainTable = relation.getMainTable();
        tableMap = Util.parseDDL(BenchmarkConfig.ddlUrl + ".perf", relation);
        if (!BenchmarkConfig.commonCol.isEmpty()) {
            String[] colArr = BenchmarkConfig.commonCol.trim().split(",");
            for (String col : colArr) {
                commonColumnIndices.add(tableMap.get(mainTable).getSchemaPos().get(col));
            }
        }
        String[] tmp = BenchmarkConfig.scriptUrl.split("/");
        String scenceName = tmp[tmp.length - 1].split("\\.")[0];
        db = scenceName;
        pName = scenceName;
        for (TableInfo table : tableMap.values()) {
            String tableName = table.getName();
            StringBuilder builder= new StringBuilder();
            builder.append("insert into ");
            builder.append(tableName);
            builder.append(" values(");
            List<String> schema = table.getSchema();
            for (int i = 0; i < schema.size(); i++) {
                builder.append("?");
                if (i != schema.size() - 1) {
                    builder.append(", ");
                }
            }
            builder.append(");");
            tableInsertSqlMap.put(tableName, builder.toString());
        }
    }

    public boolean create() {
        if (!executor.createDB(db)) {
            logger.warn("create db " + db + "failed");
            return false;
        }
        logger.info("create db " + db);
        for (TableInfo table : tableMap.values()) {
            if (!executor.executeDDL(db, table.getDDL())) {
                logger.info("create table " + table.getName() + " failed");
                return false;
            }
            logger.info("create table " + table.getName());
        }
        String procedureDDL = Util.getCreateProcedureDDL(pName, tableMap.get(mainTable), script);
        logger.info(procedureDDL);
        if (!executor.executeDDL(db, procedureDDL)) {
            logger.warn("create PROCEDURE error");
            return false;
        }
        return true;
    }

    public void clear() {
        for (String name : tableMap.keySet()) {
            executor.executeDDL(db, "drop table " + name + ";");
            logger.info("drop table " + name);
        }
        executor.dropDB(db);
        logger.info("drop db " + db);
    }

    // putTableData with random pkNum
    private void putTableData(TableInfo table) {
        putTableData(table, null);
    }

    //putTableData with given pkNum if it's not null
    private void putTableData(TableInfo table, Integer pkNum) {
        List<String> schema = table.getSchema();
        Set<Integer> index = table.getIndex();
        Set<Integer> tsIndex = table.getTsIndex();

        long ts = System.currentTimeMillis();
        int curNum = null == pkNum ? random.nextInt(BenchmarkConfig.PK_NUM) : pkNum;
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ");
        builder.append(table.getName());
        builder.append(" values(");
        for (int pos = 0; pos < schema.size(); pos++) {
            if (pos > 0) {
                builder.append(", ");
            }
            String type = schema.get(pos);
            if (type.equals("string")) {
                builder.append("'");
                builder.append("col");
                builder.append(pos);
                builder.append("-");
                builder.append(curNum);
                builder.append("'");
            } else if (type.equals("float")) {
                builder.append(1.3f);
            } else if (type.equals("double")) {
                builder.append(1.4d);
            } else if (type.equals("bigint") || type.equals("int")) {
                if (tsIndex.contains(pos)) {
                    builder.append(ts);
                } else {
                    builder.append(curNum);
                }
            } else if (type.equals("timestamp")) {
                if (tsIndex.contains(pos)) {
                    builder.append(ts);
                } else {
                    builder.append(curNum);
                }

            } else if (type.equals("bool")) {
                builder.append(true);
            } else if (type.equals("date")) {
                builder.append("'2020-11-27'");
            } else {
                logger.warn("invalid type");
            }
        }
        builder.append(");");
        String exeSql = builder.toString();
        executor.executeInsert(db, exeSql);
    }

    private boolean setInsertData(PreparedStatement requestPs, long ts, String tableName) {
        return setRequestData(requestPs, ts, tableName);
    }

    private boolean setRequestData(PreparedStatement requestPs, long ts, String tableName) {
        try {
            ResultSetMetaData metaData = requestPs.getMetaData();
            TableInfo table = null;
            if (tableName == null) {
                table = tableMap.get(mainTable);
            } else {
                table = tableMap.get(tableName);
            }
            if (table.getSchema().size() != metaData.getColumnCount()) {
                return false;
            }
            int curNum = random.nextInt(BenchmarkConfig.PK_NUM);
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    requestPs.setString(i + 1, "col" + String.valueOf(i) + "-" + String.valueOf(curNum));
                } else if (columnType == Types.DOUBLE) {
                    requestPs.setDouble(i + 1, 1.4d);
                } else if (columnType == Types.FLOAT) {
                    requestPs.setFloat(i + 1, 1.3f);
                } else if (columnType == Types.INTEGER) {
                    requestPs.setInt(i + 1, curNum);
                } else if (columnType == Types.BIGINT) {
                    if (table.getTsIndex().contains(i)) {
                        requestPs.setLong(i + 1, ts);
                    } else {
                        requestPs.setLong(i + 1, curNum);
                    }
                } else if (columnType == Types.TIMESTAMP) {
                    if (table.getTsIndex().contains(i)) {
                        requestPs.setTimestamp(i + 1, new Timestamp(ts));
                    } else {
                        requestPs.setTimestamp(i + 1, new Timestamp(curNum));
                    }
                } else if (columnType == Types.DATE) {
                    requestPs.setDate(i + 1, new Date(ts));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private PreparedStatement getPreparedStatement(BenchmarkConfig.Mode mode, boolean isProcedure) throws SQLException {
        PreparedStatement requestPs = null;
        long ts = System.currentTimeMillis();
        if (mode == BenchmarkConfig.Mode.BATCH_REQUEST) {
            if (isProcedure) {
                requestPs = executor.getCallablePreparedStmtBatch(db, pName);
            } else {
                requestPs = executor.getBatchRequestPreparedStmt(db, script, commonColumnIndices);
            }
            for (int i = 0; i < BenchmarkConfig.BATCH_SIZE; i++) {
                if (setRequestData(requestPs, ts, null)) {
                    requestPs.addBatch();
                }
            }
        } else {
            if (isProcedure) {
                requestPs = executor.getCallablePreparedStmt(db, pName);
            } else {
                requestPs = executor.getRequestPreparedStmt(db, script);
            }
            setRequestData(requestPs, ts, null);
        }
        return requestPs;
    }

    private void getData(ResultSet resultSet) {
        if (resultSet == null) {
            return;
        }
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    // String columnName = metaData.getColumnName(i + 1);
                    int columnType = metaData.getColumnType(i + 1);
                    if (columnType == Types.VARCHAR) {
                        resultSet.getString(i + 1);
                    } else if (columnType == Types.DOUBLE) {
                        resultSet.getDouble(i + 1);
                    } else if (columnType == Types.INTEGER) {
                        resultSet.getInt(i + 1);
                    } else if (columnType == Types.BIGINT) {
                        resultSet.getLong(i + 1);
                    } else if (columnType == Types.TIMESTAMP) {
                        resultSet.getTimestamp(i + 1);
                    } else if (columnType == Types.DATE) {
                        resultSet.getDate(i + 1);
                    }
                }
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public void putData() {
//        while (running.get()) {
//            for (TableInfo table : tableMap.values()) {
//                putTableData(table);
//            }
//        }
//    }
//
//    public void prePutData() {
//        for (int pkNum = 0; pkNum < BenchmarkConfig.PK_NUM; pkNum++) {
//            for (TableInfo table : tableMap.values()) {
//                putTableData(table, pkNum);
//            }
//        }
//    }

    private void insert() {
        while (running.get()) {
            if (random.nextFloat() < BenchmarkConfig.INSERT_RATIO) {
                for (TableInfo table : tableMap.values()) {
                    insertTableData(table);
                }
            } else {
                for (TableInfo table : tableMap.values()) {
                    putTableData(table);
                }
            }
        }
    }

    private void preInsert() {
        for (int pkNum = 0; pkNum < BenchmarkConfig.PK_NUM; pkNum++) {
            for (TableInfo table : tableMap.values()) {
                insertTableData(table, pkNum);
            }
        }
    }

    // putTableData with random pkNum
    private void insertTableData(TableInfo table) {
        insertTableData(table, null);
    }

    //putTableData with given pkNum if it's not null
    private void insertTableData(TableInfo table, Integer pkNum) {
        PreparedStatement ps = null;
        try {
            ps = getInsertPstmt(table, pkNum, tableInsertSqlMap.get(table.getName()));
            ps.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    private PreparedStatement getInsertPstmt(TableInfo table, Integer pkNum, String insertSql) throws SQLException{
        long ts = System.currentTimeMillis();
        PreparedStatement ps = executor.getInsertPreparedStmt(db, insertSql);
        setInsertData(ps, ts, table.getName());
        return ps;
    }

    public void query() {
        Random curRandom = new Random();
        int cnt = 0;
        while (running.get()) {
            BenchmarkConfig.Mode mode = BenchmarkConfig.Mode.REQUEST;
            if (curRandom.nextFloat() > BenchmarkConfig.REQUEST_RATIO) {
                mode = BenchmarkConfig.Mode.BATCH_REQUEST;
            }
            boolean isProcedure = false;
            if (curRandom.nextFloat() > BenchmarkConfig.PROCEDURE_RATIO) {
                isProcedure = true;
            }
            PreparedStatement ps = null;
            ResultSet resultSet = null;
            if (isProcedure) {
                try {
                    ps = null;
                    resultSet = null;
                    ps = getPreparedStatement(mode, isProcedure);
                    if (ps instanceof CallablePreparedStatement) {
                        QueryFuture future = ((CallablePreparedStatement) ps).executeQueryAsync(100, TimeUnit.MILLISECONDS);
                        resultSet = future.get();
                        getData(resultSet);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    close(ps, resultSet);
                }
            } else {
                try {
                    ps = getPreparedStatement(mode, isProcedure);
                    resultSet = ps.executeQuery();
                    getData(resultSet);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    close(ps, resultSet);
                }

            }
            try {
                String strLimit = String.valueOf(curRandom.nextInt(10000) + 1);
                String limitSql = "select * from " + mainTable + " limit " + strLimit  + ";";
                resultSet = executor.executeSQL(db, limitSql);
                getData(resultSet);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void close(PreparedStatement ps, ResultSet rs) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void runPut(int threadNum) {
        putExecuteService = Executors.newFixedThreadPool(threadNum);
        logger.info("put thread: " + threadNum);
        for (int i = 0; i < threadNum; i++) {
            putExecuteService.submit(new Runnable() {
                @Override
                public void run() {
                    insert();
                }
            });
        }
    }

    public void runQuery(int threadNum) {
        queryExecuteService = Executors.newFixedThreadPool(threadNum);
        logger.info("query thread: " + threadNum);
        for (int i = 0; i < threadNum; i++) {
            queryExecuteService.submit(new Runnable() {
                @Override
                public void run() {
                    query();
                }
            });
        }
    }

    public void close() {
        logger.info("stop run");
        running.set(false);
        queryExecuteService.shutdownNow();
        putExecuteService.shutdownNow();
        executor.close();
    }

    public static void main(String[] args) {
        FEQLFZPerf perf = new FEQLFZPerf();
        if (BenchmarkConfig.NEED_CREATE) {
            if (!perf.create()) {
                return;
            }
        }
        String[] methodArr = BenchmarkConfig.METHOD.split(",");
        for (String method : methodArr) {
            if (method.toLowerCase().equals("insert")) {
                perf.runPut(BenchmarkConfig.PUT_THREAD_NUM);
            } else if (method.toLowerCase().equals("query")) {
                perf.runQuery(BenchmarkConfig.QUERY_THREAD_NUM);
            }
        }
        //perf.putData();
        //perf.query();
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                Thread.sleep(1000);
                if (System.currentTimeMillis() - startTime > BenchmarkConfig.RUNTIME) {
                    perf.close();
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
