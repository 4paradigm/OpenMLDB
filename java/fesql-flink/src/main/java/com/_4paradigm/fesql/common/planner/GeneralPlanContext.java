package com._4paradigm.fesql.common.planner;

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
