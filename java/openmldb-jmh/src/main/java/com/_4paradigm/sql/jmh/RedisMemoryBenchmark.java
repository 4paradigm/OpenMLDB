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

package com._4paradigm.sql.jmh;

import com._4paradigm.sql.BenchmarkConfig;
import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(5)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)
public class RedisMemoryBenchmark {
    private AtomicLong counter = new AtomicLong(0l);
    private JedisPoolConfig poolConfig;
    private JedisPool pool;
    private Map<String, String> row100 = new HashMap<>();
    private Map<String, String> row200 = new HashMap<>();
    private Map<String, String> row500 = new HashMap<>();
    private String key100 = "100_key";
    private String[] field100;
    private String[] field200;
    private String[] field500;
    private String key200 = "200_key";
    private String key500 = "500_key";
    private String tpsValue = "{'tps':10000}";
    private String succValue = "{'succ':10.0}";
    private SqlExecutor executor;
    private String db = "db_mem_benchmark" + System.currentTimeMillis();
    @Setup
    public void setup() {
        SdkOption sdkOption = new SdkOption();
        sdkOption.setZkSessionTimeout(30000);
        sdkOption.setZkCluster(BenchmarkConfig.ZK_CLUSTER);
        sdkOption.setZkPath(BenchmarkConfig.ZK_PATH);
        try {
            executor = new SqlClusterExecutor(sdkOption);
            executor.createDB(db);
            executor.executeDDL(db, "create table tps_state (\n" +
                    "tps int,\n" +
                    "success_rate int,\n" +
                    "`ts` timestamp,\n" +
                    "scene string,\n" +
                    "index(key=scene, ts=`ts`)\n" +
                    ");");
        } catch (Exception e) {
            e.printStackTrace();
        }
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        pool = new JedisPool(poolConfig, BenchmarkConfig.REDIS_IP, BenchmarkConfig.REDIS_PORT);
        Jedis jedis = pool.getResource();
        jedis.flushAll();
        DateFormat df = DateFormat.getDateTimeInstance();
        long time = System.currentTimeMillis() ;
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 100000; j++) {
                String keySucc = "sense" + i + "_succ_" + "_" + df.format(new Date(time - j * 1000));
                String keyTps = "sense" + i + "_tps_" + "_" + df.format(new Date(time - j * 1000));
                jedis.set(keySucc, succValue);
                jedis.set(keyTps, tpsValue);
                try {
                    PreparedStatement ps = executor.getInsertPreparedStmt(db, "insert into tps_state values(?, ?, ?, ?);");
                    ps.setInt(1, 10000);
                    ps.setInt(2, 10);
                    ps.setTimestamp(3, new Timestamp(time - j * 1000));
                    ps.setString(4, "sense" + i);
                    ps.execute();
                } catch (Exception e){}
            }
            System.out.println("step " + i);
        }
    }
    public static void main(String[] args) throws RunnerException {
        RedisMemoryBenchmark benchmark = new RedisMemoryBenchmark();
        benchmark.setup();
    }
}
