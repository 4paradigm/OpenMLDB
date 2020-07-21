package com._4paradigm.fesql.common;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.nio.ByteBuffer;


public class FesqlPlanContext {

    private String tag;
    private BatchTableEnvironment batchTableEnvironment;
    private FesqlPlanner fesqlBatchPlanner;
    private SerializableByteBuffer moduleBuffer;

    public FesqlPlanContext(String tag, BatchTableEnvironment batchTableEnvironment, FesqlPlanner fesqlBatchPlanner, ByteBuffer moduleBuffer) {
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

    public FesqlPlanner getFesqlBatchPlanner() {
        return fesqlBatchPlanner;
    }

    public SerializableByteBuffer getModuleBuffer() {
        return moduleBuffer;
    }
}
