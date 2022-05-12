package com._4paradigm.sql.jmh.openmldb;

import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Type;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.sql.BenchmarkConfig;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)

public class DeploymentBenchmark {
    private String dbName;
    private String tableName;
    private String deployName;
    private int pkNum;
    private int pkBase;
    private long tsBase;
    private SqlExecutor executor;
    private List<Type.DataType> schema;
    private Set<Integer> index;
    private Set<Integer> tsIndex;
    private Random random;

    public DeploymentBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        dbName = BenchmarkConfig.DATABASE;
        tableName = BenchmarkConfig.TABLE;
        deployName = BenchmarkConfig.DEPLOY_NAME;
        pkNum = BenchmarkConfig.PK_NUM;
        tsBase = BenchmarkConfig.TS_BASE;
        pkBase = BenchmarkConfig.PK_BASE;
        schema = new ArrayList<>();
        index = new HashSet<>();
        tsIndex = new HashSet<>();
        random = new Random(System.currentTimeMillis());

        parseSchema();
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

    private PreparedStatement getPreparedStatement() throws SQLException {
        PreparedStatement requestPs = executor.getCallablePreparedStmt(dbName, deployName);
        ResultSetMetaData metaData = requestPs.getMetaData();
        if (schema.size() != metaData.getColumnCount()) {
            return null;
        }
        int numberKey = pkBase + random.nextInt(pkNum);
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i + 1);
            if (columnType == Types.VARCHAR) {
                if (index.contains(i)) {
                    requestPs.setString(i + 1, "key" + String.valueOf(numberKey));
                } else {
                    requestPs.setString(i + 1, "col" + String.valueOf(i));
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

    @Benchmark
    public void executeDeployment() {
        try {
            PreparedStatement stat = getPreparedStatement();
            ResultSet resultSet = stat.executeQuery();
            /*resultSet.next();
            ResultSetMetaData metaData = resultSet.getMetaData();
            Map<String, String> val = new HashMap<>();
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
            int a = 0;*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        /*DeploymentBenchmark benchmark = new DeploymentBenchmark();
        benchmark.executeDeployment();*/

        try {
            Options opt = new OptionsBuilder()
                    .include(DeploymentBenchmark.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
