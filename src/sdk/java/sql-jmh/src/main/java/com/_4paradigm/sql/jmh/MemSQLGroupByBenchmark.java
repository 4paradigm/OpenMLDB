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

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms16G", "-Xmx16G"})
@Warmup(iterations = 1)
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
                st.close();
            } catch (Exception e) { }
            try {
                st = cnn.createStatement();
                st.execute(header);
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            query = "select count (*), ";
            for (int i = 0; i < 50; i++) {
                if (i == 49) {
                    query += "avg(col_agg" + i + ")";
                }else {
                    query += "avg(col_agg" + i + "),";
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
            query += " from perf group by col1 having col1=?";
            System.out.println(query);
            for (int i = 0; i < 20000; i++) {
                String pk = "pkxxx" + i;
                for (int j = 0; j < 1000; j++) {
                    st = cnn.createStatement();
                    String sql =String.format(format, "perf", pk, System.currentTimeMillis()) ;
                    st.execute(sql);
                    st.close();
                    System.out.println(sql);
                }
                if (i % 1000 == 0) System.out.println(i * 1000);
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
            System.out.println(key);
            PreparedStatement ps = cnn.prepareStatement(query);
            ps.setString(1, key);
            ps.executeQuery();
            ps.close();
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
