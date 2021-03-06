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

package com._4paradigm.fesql.flink.common.planner;

import com._4paradigm.fesql.common.SerializableByteBuffer;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.nio.ByteBuffer;


public class GeneralPlanContext {

    private String tag;
    private BatchTableEnvironment batchTableEnvironment;
    private StreamTableEnvironment streamTableEnvironment;
    private FesqlFlinkPlanner fesqlBatchPlanner;
    private SerializableByteBuffer moduleBuffer;

    public GeneralPlanContext(String tag, BatchTableEnvironment batchTableEnvironment, FesqlFlinkPlanner fesqlBatchPlanner, ByteBuffer moduleBuffer) {
        this.tag = tag;
        this.batchTableEnvironment = batchTableEnvironment;
        this.streamTableEnvironment = null;
        this.fesqlBatchPlanner = fesqlBatchPlanner;
        this.moduleBuffer = new SerializableByteBuffer(moduleBuffer);
    }

    public GeneralPlanContext(String tag, StreamTableEnvironment streamTableEnvironment, FesqlFlinkPlanner fesqlBatchPlanner, ByteBuffer moduleBuffer) {
        this.tag = tag;
        this.batchTableEnvironment = null;
        this.streamTableEnvironment = streamTableEnvironment;
        this.fesqlBatchPlanner = fesqlBatchPlanner;
        this.moduleBuffer = new SerializableByteBuffer(moduleBuffer);
    }

    public String getTag() {
        return this.tag;
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return this.batchTableEnvironment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return this.streamTableEnvironment;
    }

    public FesqlFlinkPlanner getFesqlBatchPlanner() {
        return fesqlBatchPlanner;
    }

    public SerializableByteBuffer getModuleBuffer() {
        return moduleBuffer;
    }

}
