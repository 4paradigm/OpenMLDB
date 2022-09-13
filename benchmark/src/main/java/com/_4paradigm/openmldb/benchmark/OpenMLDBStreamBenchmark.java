package com._4paradigm.openmldb.benchmark;

import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(64)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1, time = 100)
public class OpenMLDBStreamBenchmark {
    class Row {
        Row(String key, long ts, double val, boolean isBase) {
            this.key = key;
            this.ts = ts;
            this.val = val;
            this.isBase = isBase;
        }

        String key;
        long ts;
        double val;
        boolean isBase;
    }

    private SqlExecutor executor;
    private String database;
    private String deployName;
    private long windowSize;
    private Map<String, TableSchema> tableSchema = new HashMap<>();
    private Random random;
    private List<Integer> pkList = new ArrayList<>();
    private List<AtomicLong> globalTsList = new ArrayList<>();
    private String baseName = "base";
    private String probeName = "probe";
    private String baseFile;
    private String baseKeyCol;
    private String baseTsCol;
    private String probeFile;
    private String probeKeyCol;
    private String probeTsCol;
    private String probeValCol;
    private List<Row> combinedElements = new ArrayList<>();
    private boolean genRandom = true;
    private AtomicInteger streamIdx = new AtomicInteger(0);
    private int sampleInterval = 10000;

    public OpenMLDBStreamBenchmark() {
        executor = BenchmarkConfig.GetSqlExecutor(false);
        deployName = BenchmarkConfig.DEPLOY_NAME;
        database = BenchmarkConfig.DATABASE;
        windowSize = BenchmarkConfig.WINDOW_SIZE;
        random = new Random(System.currentTimeMillis());
        for (int i = 0; i < BenchmarkConfig.PK_NUM; i++) {
            pkList.add(i);
            globalTsList.add(new AtomicLong(0));
        }

        baseFile = BenchmarkConfig.STREAM_BASE_FILE;
        baseKeyCol = BenchmarkConfig.STREAM_BASE_KEY;
        baseTsCol = BenchmarkConfig.STREAM_BASE_TS;
        probeFile = BenchmarkConfig.STREAM_PROBE_FILE;
        probeKeyCol = BenchmarkConfig.STREAM_PROBE_KEY;
        probeTsCol = BenchmarkConfig.STREAM_PROBE_TS;
        probeValCol = BenchmarkConfig.STREAM_PROBE_VAL;

        if (baseFile != null && probeFile != null) {
            if (loadData()) {
                genRandom = false;
            }
        }
    }

    private boolean loadData() {
        List<Row> probes = new ArrayList<>();
        List<Row> bases = new ArrayList<>();
        try {
            // load base data
            File myObj = new File(baseFile);
            Scanner scanner = new Scanner(myObj, "UTF-8");
            // the csv file has the first line as column names
            assert scanner.hasNextLine();
            int keyIndex = 0;
            int tsIndex = 0;
            String[] featureNames = scanner.nextLine().split(",");
            for (int i = 0; i < featureNames.length; i++) {
                if (featureNames[i].compareToIgnoreCase(baseKeyCol) == 0) {
                    keyIndex = i;
                } else if (featureNames[i].compareToIgnoreCase(baseTsCol) == 0) {
                    tsIndex = i;
                }
            }
            if (scanner.hasNextLine()) {
                while (scanner.hasNextLine()) {
                    String[] features = scanner.nextLine().split(",");
                    bases.add(new Row(features[keyIndex], Timestamp.valueOf(features[tsIndex]).getTime(), 0, true));
                }
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred during loading base stream");
            e.printStackTrace();
            return false;
        }

        try {
            // load probe data
            File myObj = new File(probeFile);
            Scanner scanner = new Scanner(myObj, "UTF-8");
            // the csv file has the first line as column names
            assert scanner.hasNextLine();
            int keyIndex = 0;
            int tsIndex = 0;
            int valIndex = 0;
            String[] featureNames = scanner.nextLine().split(",");
            for (int i = 0; i < featureNames.length; i++) {
                if (featureNames[i].compareToIgnoreCase(probeKeyCol) == 0) {
                    keyIndex = i;
                } else if (featureNames[i].compareToIgnoreCase(probeTsCol) == 0) {
                    tsIndex = i;
                } else if (featureNames[i].compareToIgnoreCase(probeValCol) == 0) {
                    valIndex = i;
                }
            }
            if (scanner.hasNextLine()) {
                while (scanner.hasNextLine()) {
                    String[] features = scanner.nextLine().split(",");
                    probes.add(new Row(features[keyIndex], Timestamp.valueOf(features[tsIndex]).getTime(), Double.parseDouble(features[valIndex]), false));
                }
            }
            scanner.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred during loading probe stream");
            e.printStackTrace();
            return false;
        }

        int baseIdx = 0, probeIdx = 0;
        while (baseIdx < bases.size() || probeIdx < probes.size()) {
            if (baseIdx < bases.size() && probeIdx < probes.size()) {
                Row base = bases.get(baseIdx);
                Row probe = probes.get(probeIdx);
                if (base.ts < probe.ts) {
                    combinedElements.add(base);
                    baseIdx++;
                } else {
                    combinedElements.add(probe);
                    probeIdx++;
                }
            } else if (baseIdx < bases.size()) {
                Row base = bases.get(baseIdx);
                combinedElements.add(base);
                baseIdx++;
            } else {
                Row probe = bases.get(probeIdx);
                combinedElements.add(probe);
                probeIdx++;
            }
        }
        return true;
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
        String ddl = Util.genStreamDDL(baseName, 1);
        Util.executeSQL(ddl, executor);
        ddl = Util.genStreamDDL(probeName, 1);
        Util.executeSQL(ddl, executor);
    }

    public void drop() {
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DROP DEPLOYMENT " + deployName + ";", executor);
        Util.executeSQL("DROP TABLE " + baseName + ";", executor);
        Util.executeSQL("DROP TABLE " + probeName + ";", executor);
        Util.executeSQL("DROP DATABASE " + database + ";", executor);
    }

    public void deploy() {
        String sql = Util.genStreamScript(baseName, probeName, windowSize);
        System.out.println(sql);
        Util.executeSQL("USE " + database + ";", executor);
        Util.executeSQL("DEPLOY " + deployName + " " + sql, executor);
    }

    public void addSchema() {
        addTableSchema(database, baseName);
        addTableSchema(database, probeName);
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
    }

    @TearDown
    public void cleanEnv() {
        drop();
    }

    @Benchmark
    public void executeDeployment() {
        if (genRandom) {
            double value = 1;
            int numberKey = 0;
            if (pkList.isEmpty()) {
                numberKey = random.nextInt(BenchmarkConfig.PK_NUM);
            } else {
                int pos = random.nextInt(pkList.size());
                numberKey = pkList.get(pos);
            }
            long ts = globalTsList.get(numberKey).incrementAndGet();
            String key = String.valueOf(numberKey);
            Util.putData(key, ts, value, tableSchema.get(baseName), executor);
            Util.putData(key, ts, value, tableSchema.get(probeName), executor);
            try {
                PreparedStatement stat = Util.getStreamPreparedStatement(deployName, key, ts, tableSchema.get(baseName), executor);
                ResultSet resultSet = stat.executeQuery();

                while (resultSet.next()) {
                    Map<String, String> val = Util.extractResultSet(resultSet);
                    System.out.println("res: " + val.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            int id = this.streamIdx.getAndIncrement();
            if (id >= combinedElements.size()) {
                streamIdx.set(1);
                id = 0;
            }
            Row ele = combinedElements.get(id);
            String key = ele.key;
            long ts = ele.ts;
            double value = ele.val;
            if (ele.isBase) {
                Util.putData(key, ts, value, tableSchema.get(baseName), executor);
            } else {
                Util.putData(key, ts, value, tableSchema.get(probeName), executor);
            }

            try {
                PreparedStatement stat = Util.getStreamPreparedStatement(deployName, key, ts, tableSchema.get(baseName), executor);
                ResultSet resultSet = stat.executeQuery();

                if (id != 0 && id % sampleInterval == 0) {
                    while (resultSet.next()) {
                        Map<String, String> val = Util.extractResultSet(resultSet);
                        System.out.println("res: " + val.toString());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("Start OpenMLDBStreamBenchmark");

        try {
            Options opt = new OptionsBuilder()
                    .include(OpenMLDBStreamBenchmark.class.getSimpleName())
                    .forks(1)
                    .build();
            new Runner(opt).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
