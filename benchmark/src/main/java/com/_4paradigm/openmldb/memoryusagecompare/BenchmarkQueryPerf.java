package com._4paradigm.openmldb.memoryusagecompare;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Threads(10)
@Fork(value = 1, jvmArgs = {"-Xms8G", "-Xmx8G"})
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 10, time = 60)
public class BenchmarkQueryPerf {
    private static final Logger logger = LoggerFactory.getLogger(BenchmarkQueryPerf.class);

    private static final RedisExecutor redis = new RedisExecutor();
    private static final OpenMLDBExecutor opdb = new OpenMLDBExecutor();

    private static final InputStream configStream = BenchmarkMemoryUsageByTalkingData.class.getClassLoader().getResourceAsStream("memory.properties");
    private static final Properties config = new Properties();
    private static String talkingDataPath = "data/train.csv";
    private CSVReader csvReader;
    private static int readBatchSize = 100000;
    private static int readDataLimit = 10000000; // 最多读取数据量

    private boolean openmldbNeedInsertData = false;
    private boolean redisNeedInsertData = false;
    private boolean parseDataRow = false;
    private String dbName = "mem";
    private String tableName = "test_query_perf";
    private String key;
    private int assertAllValueNum;
    private String specialTime;

    private int assertSpecialTimeValueNum;
    private long tsRangeMin;
    private long tsRangeMax;
    private int assertSpecialTsRangeValueNum;

    private static final AtomicInteger failures = new AtomicInteger(0);


    // 初始化OpenMLDB和Redis连接,准备测试数据
    @Setup(Level.Iteration)
    public void initEnv() throws SQLException, IOException, ParseException {
        parseConfig();
        redis.initializeJedis(config, configStream);
        redis.initJedisPool(config, configStream);
        opdb.initializeOpenMLDB(config, configStream);
        opdb.tableName = tableName;
        opdb.dbName = dbName;

        String sql = "CREATE TABLE IF NOT EXISTS `" + tableName + "`(\n" +
                "`ip` string, \n" +
                "`app` int, \n" +
                "`device` int, \n" +
                "`os` int, \n" +
                "`channel` int, \n" +
                "`click_time` Timestamp, \n" +
                "`is_attributed` int, \n" +
                "INDEX (KEY=`ip`, TS=`click_time`) \n" +
                ") OPTIONS (PARTITIONNUM=8, REPLICANUM=1, STORAGE_MODE='Memory', COMPRESS_TYPE='NoCompress');";


        if (openmldbNeedInsertData && redisNeedInsertData) {
            redis.clear();
            opdb.initOpenMLDBEnvWithDDL(sql);
            csvReader = new CSVReader(talkingDataPath);
            insertData(true, true);
        } else if (redisNeedInsertData) {
            redis.clear();
            opdb.initOpenMLDBEnv();
            csvReader = new CSVReader(talkingDataPath);
            insertData(false, true);
        } else {
            opdb.initOpenMLDBEnv();
            logger.warn(
                    "Skip insert data into Redis and OpenMLDB. " +
                            "You need to ensure that the test data is already in the target table"
            );
        }
    }

    private void parseConfig() throws IOException {
        config.load(configStream);
        talkingDataPath = config.getProperty("TALKING_DATASET_PATH");
        readBatchSize = Integer.parseInt(config.getProperty("READ_DATA_BATCH_SIZE"));
        readDataLimit = Integer.parseInt(config.getProperty("READ_DATA_LIMIT"));

        openmldbNeedInsertData = Boolean.parseBoolean(config.getProperty("OPENMLDB_NEED_INSERT_DATA"));
        redisNeedInsertData = Boolean.parseBoolean(config.getProperty("REDIS_NEED_INSERT_DATA"));
        parseDataRow = Boolean.parseBoolean(config.getProperty("PARSE_DATA_ROW"));
        dbName = config.getProperty("QUERY_PERF_TEST_DB");
        tableName = config.getProperty("QUERY_PERF_TEST_TABLE");
        assertAllValueNum = Integer.parseInt(config.getProperty("ASSERT_ALL_VALUE_NUM"));


        key = config.getProperty("QUERY_KEY");
        specialTime = config.getProperty("QUERY_TIME");
        assertSpecialTimeValueNum = Integer.parseInt(config.getProperty("ASSERT_QUERY_TIME_VALUE_NUM"));
        LocalDate date = LocalDate.parse(config.getProperty("QUERY_DATE"));
        tsRangeMin = date.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        tsRangeMax = tsRangeMin + 24 * 3600 * 1000;
        assertSpecialTsRangeValueNum = Integer.parseInt(config.getProperty("ASSERT_QUERY_DATE_VALUE_NUM"));
    }

    private void insertData(boolean opdbInsert, boolean redisInsert) throws ParseException {
        logger.info("Start to insert data into Redis and OpenMLDB.");
        for (int curr = 0; curr < readDataLimit; ) {
            HashMap<String, ArrayList<TalkingData>> testData = csvReader.readCSV(readBatchSize);
            int size = getDataSize(testData);
            if (opdbInsert) opdb.insertTalkingData(testData);
            if (redisInsert) redis.insertTalkingData(testData);
            curr += size;
            logger.info("insert data into Redis and OpenMLDB, current size: {}", curr);
            if (size < readBatchSize) {
                System.out.println("end of csv file.");
                break;
            }
        }
        logger.info("insert data into Redis and OpenMLDB finished.");
    }

    private int getDataSize(HashMap<String, ArrayList<TalkingData>> data) {
        int size = 0;
        for (String key : data.keySet()) {
            size += data.get(key).size();
        }
        return size;
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        opdb.close();
        redis.close();
        System.out.println("failures: " + failures);
    }

    // 查询指定 key 对应所有的 value
    @Benchmark
    public void testOpenMLDBGetAllValues() {
        String sql = "select * from `" + opdb.tableName + "` where ip='" + key + "';";
        int size;
        if (parseDataRow) {
            ArrayList<HashMap<String, Object>> res = opdb.queryRowsWithSql(sql);
            size = res.size();
        } else {
            size = opdb.queryRowSizeWithSql(sql);
        }

        if (size != assertAllValueNum) {
            failures.incrementAndGet();
            System.out.println("testOpenMLDBGetAllValues fail.");
        }
    }

    @Benchmark
    public void testRedisGetAllValues() {
        List<String> res = redis.queryAllData(key);
        if (res.size() != assertAllValueNum) {
            failures.incrementAndGet();
            System.out.println("testRedisGetAllValues fail.");
        }
    }

    // 查询指定 key 和 指定 ts 的 value
    @Benchmark
    public void testOpenMLDBGetOneValue() {
        String sql = "select * from `" + opdb.tableName + "` where ip='" + key + "' and click_time='" + specialTime + "';";
        int size;
        if (parseDataRow) {
            ArrayList<HashMap<String, Object>> res = opdb.queryRowsWithSql(sql);
            size = res.size();

        } else {
            size = opdb.queryRowSizeWithSql(sql);
        }
        if (size != assertSpecialTimeValueNum) {
            failures.incrementAndGet();
            System.out.println("testOpenMLDBGetOneValue fail.");
        }
    }

    @Benchmark
    public void testRedisGetOneValue() throws ParseException {
        double ts = Utils.getTimestamp(specialTime);
        List<String> res = redis.queryDataWithScore(key, ts);
        if (res.size() != assertSpecialTimeValueNum) {
            failures.incrementAndGet();
            System.out.println("testRedisGetOneValue fail.");
        }
    }

    // 查询指定 key 在指定日期的所有 values，即指定 key ，在 ts 在一定范围内的所有 value。
    @Benchmark
    public void testOpenMLDBGetDateValues() {
        String sql = "select * from `" + opdb.tableName + "` where ip='" + key + "' and click_time>=timestamp(" + tsRangeMin + ") and click_time<timestamp(" + tsRangeMax + ");";
        int size;
        if (parseDataRow) {
            ArrayList<HashMap<String, Object>> res = opdb.queryRowsWithSql(sql);
            size = res.size();
        } else {
            size = opdb.queryRowSizeWithSql(sql);
        }
        if (size != assertSpecialTsRangeValueNum) {
            failures.incrementAndGet();
            System.out.println("testOpenMLDBGetDateValues fail.");
        }
    }

    @Benchmark
    public void testRedisGetDateValues() {
        List<String> res = redis.queryDataRangeByScores(key, tsRangeMin, tsRangeMax);
        if (res.size() != assertSpecialTsRangeValueNum) {
            failures.incrementAndGet();
            System.out.println("testRedisGetDateValues fail.");
        }
    }

    public static void main(String[] args) throws Exception {
        /*BenchmarkQueryPerf ben = new BenchmarkQueryPerf();
        ben.initEnv();
        ben.testOpenMLDBGetAllValues();
        ben.testRedisGetAllValues();

        ben.testOpenMLDBGetOneValue();
        ben.testRedisGetOneValue();

        ben.testOpenMLDBGetDateValues();
        ben.testRedisGetDateValues();
        for(int i=0; i < 100; i++ ) {
            ben.testRedisGetAllValues();
            logger.info("{}", i);
        }
        System.out.println(failures);*/

        Options opt = new OptionsBuilder()
                .include(BenchmarkQueryPerf.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();

        System.out.println("failures: " + failures);
    }
}
