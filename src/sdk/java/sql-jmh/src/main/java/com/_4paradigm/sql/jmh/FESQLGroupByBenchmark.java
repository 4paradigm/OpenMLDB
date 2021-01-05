package com._4paradigm.sql.jmh;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.sql.sdk.SdkOption;
import com._4paradigm.sql.sdk.SqlExecutor;
import com._4paradigm.sql.sdk.impl.SqlClusterExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(2)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)
public class FESQLGroupByBenchmark {
    private ArrayList<String> dataset = new ArrayList<>();
    private SqlExecutor executor;
    private SdkOption option;
    private String db = "db" + System.nanoTime();
    private String ddl = "";
    private String query = "";
    private boolean setupOk = false;
    private int recordSize = 1000;
    private String format = "insert into %s values('%s', %d,";
    private long counter = 0;
    public FESQLGroupByBenchmark() {
        SdkOption sdkOption = new SdkOption();
        sdkOption.setSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        this.option = sdkOption;
        try {
            executor = new SqlClusterExecutor(option);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Setup
    public void setup() {
        String header = "create table perf (col1 string, col2 bigint, ";
        for (int i = 0; i < 50; i++) {
            header += "col_agg" + i + " double,";
        }
        header += "index(key=col1, ts=col2));" ;
        ddl = header;
        query = "select ";
        for (int i = 0; i < 50; i++) {
            if (i == 49) {
                query += "sum(col_agg" + i + ")";
            }else {
                query += "sum(col_agg" + i + "),";
            }
        }
        query += " from perf group by col1;";
        for (int i = 0; i < 50; i++) {
            if (i == 49) {
                format += "2.0";
            }else {
                format += "2.0,";
            }
        }
        format+=");";
        setupOk = executor.createDB(db);
        if (!setupOk) {
            return;
        }
        setupOk = executor.executeDDL(db, ddl);
        if (!setupOk) {
            return;
        }
        for (int i = 0; i < recordSize; i++) {
            String sql = String.format(format, "perf","pkxxx", System.currentTimeMillis());
            System.out.println(executor.executeInsert(db, sql));
        }
    }

    @Benchmark
    public void groupByBm() {
        executor.executeSQL(db, query);
    }
    public static void main(String[] args) throws RunnerException {
       Options opt = new OptionsBuilder()
                .include(FESQLGroupByBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
