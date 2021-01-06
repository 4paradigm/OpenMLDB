package com._4paradigm.sql.jmh;

import com._4paradigm.sql.BenchmarkConfig;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 2)
public class MemSQLGroupByBenchmark {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private ArrayList<String> dataset = new ArrayList<>();
    private ArrayList<String> querySet = new ArrayList<>();
    private Connection cnn;
    private String query;
    private String format = "insert into %s values('%s', %d,";
    private long counter = 0;
    private Random random = new Random(System.currentTimeMillis());
    public MemSQLGroupByBenchmark() {
    }
    @Setup
    public void setup() {
        try {
            cnn = DriverManager.getConnection(BenchmarkConfig.MEMSQL_URL);
            cnn.setAutoCommit(true);
            Statement st = cnn.createStatement();
            String header = "create table perf (col1 varchar(20), col2 bigint,";
            for (int i = 0; i < 50; i++) {
                header += "col_agg" + i + " double,";
            }
            header += "key(col1, col2))" ;
            System.out.println(header);
            try {
                st.execute("drop table perf");
            } catch (Exception e) { }
            try {
                cnn.createStatement().execute(header);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            query = "select ";
            for (int i = 0; i < 50; i++) {
                if (i == 49) {
                    query += "sum(col_agg" + i + ")";
                }else {
                    query += "sum(col_agg" + i + "),";
                }
            }
            for (int i = 0; i < 50; i++) {
                if (i == 49) {
                    format += "2.0";
                }else {
                    format += "2.0,";
                }
            }
            format+=");";
            query += " from perf where col1 = ? group by col1";
            for (int i = 0; i < 20000; i++) {
                String pk = "pkxxx" + i;
                for (int j = 0; j < 100; j++) {
                    st = cnn.createStatement();
                    String sql =String.format(format, "perf", pk, System.currentTimeMillis()) ;
                    st.execute(sql);
                }
                if (i % 1000 == 0) System.out.println(i * 100);
                querySet.add(pk);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Benchmark
    public void groupByBm() {
        long index = random.nextInt(querySet.size());
        try {
            if (index < 0) index = index * -1;
            String key = querySet.get((int) index);
            PreparedStatement ps = cnn.prepareStatement(query);
            ps.setString(1, key);
            ps.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            counter++;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MemSQLGroupByBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
