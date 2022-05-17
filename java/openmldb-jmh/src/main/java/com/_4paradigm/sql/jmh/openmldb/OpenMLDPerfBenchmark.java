package com._4paradigm.sql.jmh.openmldb;

import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.sql.tools.Util;
import com._4paradigm.sql.tools.TableSchema;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Threads(10)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 2)
@Measurement(iterations = 1, time = 20)

public class OpenMLDPerfBenchmark {
    private SqlExecutor executor;
    private String database;
    private String deployName;
    private int windowNum;
    private int windowSize;
    private int joinNum;
    private int unionNum = 0; // unspport in cluster mode now
    private Map<String, TableSchema> tableSchema = new HashMap<>();
    private Random random;

    public OpenMLDPerfBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        deployName = BenchmarkConfig.DEPLOY_NAME;
        database = BenchmarkConfig.DATABASE;
        joinNum = BenchmarkConfig.JOIN_NUM;
        windowNum = BenchmarkConfig.WINDOW_NUM;
        windowSize = BenchmarkConfig.WINDOW_SIZE;
        random = new Random(System.currentTimeMillis());
    }

    private void addTableSchema(String dbName, String tableName) {
        NS.TableInfo tableInfo = null;
        try {
            tableInfo = executor.getTableInfo(dbName, tableName);
            TableSchema schema = new TableSchema(tableInfo);
            tableSchema.put(tableName, schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void create () {
        Util.executeSQL("CREATE DATABASE IF NOT EXISTS " + database + ";", executor);
        Util.executeSQL("USE " + database + ";", executor);
        String ddl = Util.genDDL("mt", windowNum);
        Util.executeSQL(ddl, executor);
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            ddl = Util.genDDL(tableName, windowNum);
            Util.executeSQL(ddl, executor);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            ddl = Util.genDDL(tableName, 1);
            Util.executeSQL(ddl, executor);
        }
    }

    public void putData() {
        Util.loadData(tableSchema.get("mt"), windowSize, executor);
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            Util.loadData(tableSchema.get(tableName), windowSize, executor);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            Util.loadData(tableSchema.get(tableName), windowSize, executor);
        }
    }

    public void drop() {
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DROP DEPLOYMENT " + deployName + ";", executor);
        Util.executeSQL("DROP TABLE mt;", executor);
        for (int i = 0; i < unionNum; i++) {
            Util.executeSQL("DROP TABLE ut" + String.valueOf(i) + ";", executor);
        }
        for (int i = 0; i < joinNum; i++) {
            Util.executeSQL("DROP TABLE lt" + String.valueOf(i) + ";", executor);
        }
        Util.executeSQL("DROP DATABASE " + database + ";", executor);
    }

    public void deploy() {
        String sql = Util.genScript(windowNum, windowSize, unionNum, joinNum);
        System.out.println(sql);
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DEPLOY " + deployName + " " + sql, executor);
    }

    public void addSchema() {
        addTableSchema(database, "mt");
        for (int i = 0; i < unionNum; i++) {
            String tableName = "ut" + String.valueOf(i);
            addTableSchema(database, tableName);
        }
        for (int i = 0; i < joinNum; i++) {
            String tableName = "lt" + String.valueOf(i);
            addTableSchema(database, tableName);
        }
    }

    @Setup
    public void initEnv() {
        create();
        deploy();
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        addSchema();
        putData();
    }

    @TearDown
    public void cleanEnv() {
        drop();
    }

    @Benchmark
    public void executeDeployment() {
        int numberKey = BenchmarkConfig.PK_BASE + random.nextInt(BenchmarkConfig.PK_NUM);
        try {
            PreparedStatement stat = Util.getPreparedStatement(deployName, numberKey, tableSchema.get("mt"), executor);
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
        /*OpenMLDPerfBenchmark benchmark = new OpenMLDPerfBenchmark();
        benchmark.initEnv();
        benchmark.executeDeployment();
        benchmark.cleanEnv();*/

        try {
            Options opt = new OptionsBuilder()
                    .include(OpenMLDPerfBenchmark.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
