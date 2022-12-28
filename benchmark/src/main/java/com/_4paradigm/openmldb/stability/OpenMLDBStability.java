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

package com._4paradigm.openmldb.stability;

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Type;
import com._4paradigm.openmldb.sdk.QueryFuture;
import com._4paradigm.openmldb.jdbc.CallablePreparedStatement;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OpenMLDBStability {
    private static Logger logger = LoggerFactory.getLogger(OpenMLDBStability.class);
    private SqlExecutor executor;
    private String db;
    private String pName;
    private String script;
    private String ddl;
    private String mainTable = "flattenRequest";
    private Map<String, TableInfo> tableMap = new HashMap<>();
    private Map<String, String> tableInsertSqlMap = new HashMap<>();

    private ExecutorService putExecuteService;
    private ExecutorService queryExecuteService;
    private Random random = new Random();
    private AtomicBoolean running = new AtomicBoolean(true);

    public OpenMLDBStability() {
        executor = Config.GetSqlExecutor(false);
        this.db = Config.DB_NAME;
        this.pName = Config.CASE_NAME;
        init();
    }

    public void init() {
        String baseDir = this.getClass().getClassLoader().getResource("").getPath() + "/" + Config.CASE_PATH + "/" + Config.CASE_NAME;
        String rawScript = FileUtil.ReadFile(baseDir + "/sql.txt");
        script = rawScript.trim().replace("\n", " ");
        ddl = FileUtil.ReadFile(baseDir + "/ddl.txt");
    }

    public boolean create() {
        Statement statement = executor.getStatement();
        try {
            statement.execute("CREATE DATABASE IF NOT EXISTS " + db + ";");
            logger.info("create db " + db);
            statement.execute("USE " + db + ";");
            String[] arr = ddl.split(";");
            for (String item : arr) {
                if (item.trim().isEmpty()) {
                    continue;
                }
                String storageMode = Config.STORAGE_MODE.equals("memory") ? "" : ", STORAGE_MODE='HDD'";
                String curSQL = item + " OPTIONS ( PARTITIONNUM=" + Config.PARTITION_NUM + ", REPLICANUM=" + Config.REPLICA_NUM + storageMode + ");";
                statement.execute(curSQL);
                logger.info(curSQL + " execute ok!");
            }
            String deploySQL = "DEPLOY " + pName + " " + script;
            logger.info(deploySQL);
            statement.execute(deploySQL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public void getTableInfo() {
        List<String> tables = executor.getTableNames(db);
        try {
            for (String name : tables) {
                NS.TableInfo tableInfo = executor.getTableInfo(db, name);
                tableMap.put(name, new TableInfo(tableInfo));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void clear() {
        Statement statement = executor.getStatement();
        try {
            statement.execute("use " + db + ";");
            statement.execute("drop deployment " + pName + ";");
            for (String name : tableMap.keySet()) {
                statement.execute("drop table " + name + ";");
                logger.info("drop table " + name);
            }
            statement.execute("drop database " + db + ";");
            logger.info("drop db " + db);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // putTableData with random pkNum
    private void putTableData(TableInfo table) {
        putTableData(table, null);
    }

    //putTableData with given pkNum if it's not null
    private void putTableData(TableInfo table, Integer pkNum) {

        long ts = System.currentTimeMillis();
        int curNum = null == pkNum ? random.nextInt(Config.PK_NUM) : pkNum;
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ");
        builder.append(table.getName());
        builder.append(" values(");
        List<Common.ColumnDesc> schema = table.getSchema();
        for (int pos = 0; pos < schema.size(); pos++) {
            if (pos > 0) {
                builder.append(", ");
            }
            Type.DataType type = schema.get(pos).getDataType();
            if (type == Type.DataType.kString || type == Type.DataType.kVarchar) {
                builder.append("'");
                builder.append("key");
                builder.append("-");
                builder.append(curNum);
                builder.append("'");
            } else if (type == Type.DataType.kFloat) {
                builder.append(random.nextFloat());
            } else if (type == Type.DataType.kDouble) {
                builder.append(random.nextDouble());
            } else if (type == Type.DataType.kBigInt) {
                if (table.isTsCol(schema.get(pos).getName())) {
                    builder.append(ts);
                } else {
                    builder.append(curNum);
                }
            } else if (type == Type.DataType.kInt || type == Type.DataType.kSmallInt) {
                builder.append(curNum);
            } else if (type == Type.DataType.kTimestamp) {
                builder.append(ts);
            } else if (type == Type.DataType.kBool) {
                builder.append(true);
            } else if (type == Type.DataType.kDate) {
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
            int curNum = random.nextInt(Config.PK_NUM);
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                int columnType = metaData.getColumnType(i + 1);
                if (columnType == Types.VARCHAR) {
                    requestPs.setString(i + 1, "key" + "-" + String.valueOf(curNum));
                } else if (columnType == Types.DOUBLE) {
                    requestPs.setDouble(i + 1, random.nextDouble());
                } else if (columnType == Types.FLOAT) {
                    requestPs.setFloat(i + 1, random.nextFloat());
                } else if (columnType == Types.INTEGER) {
                    requestPs.setInt(i + 1, curNum);
                } else if (columnType == Types.BIGINT) {
                    if (table.isTsCol(metaData.getColumnName(i + 1))) {
                        requestPs.setLong(i + 1, ts);
                    } else {
                        requestPs.setLong(i + 1, curNum);
                    }
                } else if (columnType == Types.TIMESTAMP) {
                    requestPs.setTimestamp(i + 1, new Timestamp(ts));
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

    private PreparedStatement getPreparedStatement(boolean isProcedure) throws SQLException {
        PreparedStatement requestPs = null;
        long ts = System.currentTimeMillis();
        /*if (mode == BenchmarkConfig.Mode.BATCH_REQUEST) {
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
        }*/
        if (isProcedure) {
            requestPs = executor.getCallablePreparedStmt(db, pName);
        } else {
            requestPs = executor.getRequestPreparedStmt(db, script);
        }
        setRequestData(requestPs, ts, null);
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
                        String val = resultSet.getString(i + 1);
                    } else if (columnType == Types.DOUBLE) {
                        double val = resultSet.getDouble(i + 1);
                    } else if (columnType == Types.INTEGER) {
                        int val = resultSet.getInt(i + 1);
                    } else if (columnType == Types.BIGINT) {
                        long val = resultSet.getLong(i + 1);
                    } else if (columnType == Types.TIMESTAMP) {
                        Timestamp ts = resultSet.getTimestamp(i + 1);
                    } else if (columnType == Types.DATE) {
                        Date date = resultSet.getDate(i + 1);
                    }
                }
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void putData() {
        //while (running.get()) {
            for (TableInfo table : tableMap.values()) {
                putTableData(table);
            }
        //}
    }

    private void insert() {
        while (running.get()) {
            /*if (random.nextFloat() < Config.INSERT_RATIO) {
                for (TableInfo table : tableMap.values()) {
                    insertTableData(table);
                }
            } else {*/
                for (TableInfo table : tableMap.values()) {
                    putTableData(table);
                }
            //}
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
            boolean isProcedure = true;
            //if (curRandom.nextFloat() > BenchmarkConfig.PROCEDURE_RATIO) {
            //    isProcedure = true;
            //}
            PreparedStatement ps = null;
            ResultSet resultSet = null;
            boolean isSync = curRandom.nextBoolean();
            if (!isSync) {
                try {
                    ps = null;
                    resultSet = null;
                    ps = getPreparedStatement(isProcedure);
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
                    ps = getPreparedStatement(isProcedure);
                    resultSet = ps.executeQuery();
                    getData(resultSet);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    close(ps, resultSet);
                }

            }
            /*try {
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
            }*/
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
        OpenMLDBStability stability = new OpenMLDBStability();
        if (Config.NEED_CREATE) {
            if (!stability.create()) {
                return;
            }
        }
        stability.getTableInfo();
        //stability.query();
        //stability.clear();
        if (Config.ENABLE_PUT) {
            stability.runPut(Config.PUT_THREAD_NUM);
        }
        if (Config.ENABLE_QUERY) {
            stability.runQuery(Config.QUERY_THREAD_NUM);
        }
        //stability.putData();
        //perf.query();
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                Thread.sleep(1000);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
