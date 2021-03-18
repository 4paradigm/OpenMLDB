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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Threads(5)
@Fork(value = 1, jvmArgs = {"-Xms32G", "-Xmx32G"})
@Warmup(iterations = 1)
public class RedisHMBenchmark {
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
    @Setup
    public void setup() {
        poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        pool = new JedisPool(poolConfig, BenchmarkConfig.REDIS_IP, BenchmarkConfig.REDIS_PORT);
        Jedis jedis = pool.getResource();
        jedis.flushAll();

        for (int i = 0; i < 100; i++) {
            row100.put("column" + String.valueOf(i), "value100000000" + String.valueOf(i));
        }
        field100 = row100.keySet().toArray(new String[0]);
        jedis.hmset(key100, row100);
        for (int i = 0; i < 200; i++) {
            row200.put("column" + String.valueOf(i), "value100000000" + String.valueOf(i));
        }
        field200 = row200.keySet().toArray(new String[0]);
        jedis.hmset(key200, row200);
        for (int i = 0; i < 500; i++) {
            row500.put("column" + String.valueOf(i), "value100000000" + String.valueOf(i));
        }
        field500 = row500.keySet().toArray(new String[0]);
        jedis.hmset(key500, row500);
        jedis.close();
    }

    @Benchmark
    public void hm100SetRead() {
        Jedis jedis = pool.getResource();
        jedis.hmget(key100, field100);
        jedis.close();

    }
    @Benchmark
    public void hm200SetRead() {
        Jedis jedis = pool.getResource();
        jedis.hmget(key200, field200);
        jedis.close();
    }
    @Benchmark
    public void hm500SetRead() {
        Jedis jedis = pool.getResource();
        jedis.hmget(key500, field500);
        jedis.close();
    }

    @Benchmark
    public void hm100SetWrite() {
        Jedis jedis = pool.getResource();
        String key = "100"+ String.valueOf(counter.incrementAndGet());
        jedis.hmset(key, row100);
        jedis.close();
    }

    @Benchmark
    public void hm200SetWrite() {
        Jedis jedis = pool.getResource();
        String key = "200"+ String.valueOf(counter.incrementAndGet());
        jedis.hmset(key, row200);
        jedis.close();
    }

    @Benchmark
    public void hm500SetWrite() {
        Jedis jedis = pool.getResource();
        String key = "500"+ String.valueOf(counter.incrementAndGet());
        jedis.hmset(key, row500);
        jedis.close();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RedisHMBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
