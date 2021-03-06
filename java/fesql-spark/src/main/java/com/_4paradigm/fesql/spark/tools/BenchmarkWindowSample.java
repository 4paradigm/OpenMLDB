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

package com._4paradigm.fesql.spark.tools;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class BenchmarkWindowSample {

    static private BenchmarkWindowSampleImpl bm = new BenchmarkWindowSampleImpl();

    @Setup
    public void initExecutor() {
        bm.parseArgs(bm.loadArgs());
        bm.initExecutor();
    }

    @Benchmark
    public void runWindowSample() {
        bm.runWindowSample();
    }

    static public void main(String[] args) {
        bm.run(BenchmarkWindowSample.class, args);
    }
}
