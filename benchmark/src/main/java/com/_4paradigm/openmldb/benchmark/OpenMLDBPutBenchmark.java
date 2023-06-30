package com._4paradigm.openmldb.benchmark;

import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Timestamp;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 1)
@Measurement(iterations = 1, time = 30)

public class OpenMLDBPutBenchmark {
    private SqlExecutor executor;
    private String database = "test_put_db";
    private String tableName = "test_put_t1";
    private int indexNum;
    private String placeholderSQL;
    private Random random;
    int stringNum = 15;
    int doubleNum= 5;
    int timestampNum = 5;
    int bigintNum = 5;

    public OpenMLDBPutBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        indexNum = BenchmarkConfig.WINDOW_NUM;
        random = new Random();
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ");
        builder.append(tableName);
        builder.append(" values (");
        for (int i = 0; i < stringNum + doubleNum + timestampNum + bigintNum; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("?");
        }
        builder.append(");");
        placeholderSQL = builder.toString();
    }

    @Setup
    public void initEnv() {
        Util.executeSQL("CREATE DATABASE IF NOT EXISTS " + database + ";", executor);
        Util.executeSQL("USE " + database + ";", executor);
        String ddl = Util.genDDL(tableName, indexNum);
        Util.executeSQL(ddl, executor);
    }

    @Benchmark
    public void executePut() {
        java.sql.PreparedStatement pstmt = null;
        try {
            pstmt = executor.getInsertPreparedStmt(database, placeholderSQL);
            for (int num = 0; num < BenchmarkConfig.PUT_BACH_SIZE; num++) {
                int idx = 1;
                for (int i = 0; i < stringNum; i++) {
                    if (i < indexNum) {
                        pstmt.setString(idx, String.valueOf(BenchmarkConfig.PK_BASE + random.nextInt(BenchmarkConfig.PK_NUM)));
                    } else {
                        pstmt.setString(idx, "aabbcc");
                    }
                    idx++;
                }
                for (int i = 0; i < doubleNum; i++) {
                    pstmt.setDouble(idx, 1.4f);
                    idx++;
                }
                for (int i = 0; i < timestampNum; i++) {
                    pstmt.setTimestamp(idx, new Timestamp(System.currentTimeMillis()));
                    idx++;
                }
                for (int i = 0; i < bigintNum; i++) {
                    pstmt.setLong(idx, System.currentTimeMillis());
                    idx++;
                }
                if (BenchmarkConfig.PUT_BACH_SIZE > 1) {
                    pstmt.addBatch();
                }
            }
            if (BenchmarkConfig.PUT_BACH_SIZE > 1) {
                pstmt.executeBatch();
            } else {
                pstmt.execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @TearDown
    public void cleanEnv() {
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DROP TABLE " + tableName + ";", executor);
        Util.executeSQL("DROP DATABASE " + database + ";", executor);
    }

    public static void main(String[] args) {
       /* OpenMLDBPutBenchmark benchmark = new OpenMLDBPutBenchmark();
        benchmark.initEnv();
        benchmark.executePut();
        benchmark.cleanEnv();*/

        try {
            Options opt = new OptionsBuilder()
                    .include(OpenMLDBPutBenchmark.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
