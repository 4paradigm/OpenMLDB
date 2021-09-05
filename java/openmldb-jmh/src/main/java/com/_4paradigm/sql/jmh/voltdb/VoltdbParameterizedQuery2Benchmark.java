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

package com._4paradigm.sql.jmh.voltdb;

import com._4paradigm.sql.jmh.ParameterizedQuery2Benchmark;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 1)
@Slf4j
public class VoltdbParameterizedQuery2Benchmark extends VoltdbSetup implements ParameterizedQuery2Benchmark {
    @Override
    @Setup(Level.Trial)
    public void setup() throws SQLException {
        super.setup();
        prepareData();
    }

    @Override
    @TearDown(Level.Trial)
    public void teardown() throws SQLException {
        cleanup();
        super.teardown();
    }

    @Benchmark
    public ResultSet bm() throws SQLException {
        return query();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VoltdbParameterizedQuery2Benchmark.class.getSimpleName())
                .shouldFailOnError(true)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
