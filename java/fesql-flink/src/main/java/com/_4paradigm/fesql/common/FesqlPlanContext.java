package com._4paradigm.fesql.common;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.nio.ByteBuffer;


public class FesqlPlanContext {

    private String tag;
    private BatchTableEnvironment batchTableEnvironment;
    private StreamTableEnvironment streamTableEnvironment;
    private FesqlPlanner fesqlBatchPlanner;
    private SerializableByteBuffer moduleBuffer;

    public FesqlPlanContext(String tag, BatchTableEnvironment batchTableEnvironment, FesqlPlanner fesqlBatchPlanner, ByteBuffer moduleBuffer) {
        this.tag = tag;
        this.batchTableEnvironment = batchTableEnvironment;
        this.streamTableEnvironment = null;
        this.fesqlBatchPlanner = fesqlBatchPlanner;
        this.moduleBuffer = new SerializableByteBuffer(moduleBuffer);
    }

    public FesqlPlanContext(String tag, StreamTableEnvironment streamTableEnvironment, FesqlPlanner fesqlBatchPlanner, ByteBuffer moduleBuffer) {
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

    public FesqlPlanner getFesqlBatchPlanner() {
        return fesqlBatchPlanner;
    }

    public SerializableByteBuffer getModuleBuffer() {
        return moduleBuffer;
    }

}
