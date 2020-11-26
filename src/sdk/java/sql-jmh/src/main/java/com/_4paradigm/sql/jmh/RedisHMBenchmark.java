package com._4paradigm.sql.jmh;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms16G", "-Xmx16G"})
@Warmup(iterations = 1)
public class RedisHMBenchmark {
    private AtomicLong counter = new AtomicLong(0l);
    private Jedis jedis;
    private Map<String, String> row100 = new HashMap<>();
    private Map<String, String> row200 = new HashMap<>();
    private Map<String, String> row500 = new HashMap<>();
    @Setup
    public void setup() {
        jedis = new Jedis(BenchmarkConfig.REDIS_IP, BenchmarkConfig.REDIS_PORT);
        jedis.flushAll();
        for (int i = 0; i < 100; i++) {
            row100.put("column" + String.valueOf(i), "value" + String.valueOf(i));
        }
        for (int i = 0; i < 200; i++) {
            row200.put("column" + String.valueOf(i), "value" + String.valueOf(i));
        }
        for (int i = 0; i < 500; i++) {
            row500.put("column" + String.valueOf(i), "value" + String.valueOf(i));
        }
    }

    @Benchmark
    public void hm100Set() {
        String key = "100"+ String.valueOf(counter.incrementAndGet());
        jedis.hmset(key, row100);
    }

    @Benchmark
    public void hm200Set() {
        String key = "200"+ String.valueOf(counter.incrementAndGet());
        jedis.hmset(key, row200);
    }

    @Benchmark
    public void hm500Set() {
        String key = "500"+ String.valueOf(counter.incrementAndGet());
        jedis.hmset(key, row500);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RedisHMBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
