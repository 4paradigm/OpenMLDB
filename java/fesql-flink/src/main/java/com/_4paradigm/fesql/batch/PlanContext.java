package com._4paradigm.fesql.batch;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.nio.ByteBuffer;


public class PlanContext {

    private String tag;
    private BatchTableEnvironment batchTableEnvironment;
    private FesqlBatchPlanner fesqlBatchPlanner;
    private SerializableByteBuffer moduleBuffer;

    public PlanContext(String tag, BatchTableEnvironment batchTableEnvironment, FesqlBatchPlanner fesqlBatchPlanner, ByteBuffer moduleBuffer) {
        this.tag = tag;
        this.batchTableEnvironment = batchTableEnvironment;
        this.fesqlBatchPlanner = fesqlBatchPlanner;
        this.moduleBuffer = new SerializableByteBuffer(moduleBuffer);
    }

    public String getTag() {
        return this.tag;
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return this.batchTableEnvironment;
    }

    public FesqlBatchPlanner getFesqlBatchPlanner() {
        return fesqlBatchPlanner;
    }

    public SerializableByteBuffer getModuleBuffer() {
        return moduleBuffer;
    }
}
