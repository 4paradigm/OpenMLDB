package com._4paradigm.fesql.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

public class FesqlStreamTableEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FesqlStreamTableEnvironment.class);

    private StreamTableEnvironment streamTableEnvironment;

    private Map<String, TableSchema> registeredTableSchemaMap = new HashMap<String, TableSchema>();

    public FesqlStreamTableEnvironment(StreamTableEnvironment streamTableEnvironment) {
        this.streamTableEnvironment = streamTableEnvironment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return this.streamTableEnvironment;
    }

    public Map<String, TableSchema> getRegisteredTableSchemaMap() {
        return this.registeredTableSchemaMap;
    }


    public static FesqlStreamTableEnvironment create(StreamTableEnvironment streamTableEnvironment) {
        return new FesqlStreamTableEnvironment(streamTableEnvironment);
    }

    public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
        this.streamTableEnvironment.registerFunction(name, tableFunction);
    }


    public <T, ACC> void registerFunction(String name, AggregateFunction<T, ACC> aggregateFunction) {
        this.streamTableEnvironment.registerFunction(name, aggregateFunction);
    }

    public <T, ACC> void registerFunction(String name, TableAggregateFunction<T, ACC> tableAggregateFunction) {
        this.streamTableEnvironment.registerFunction(name, tableAggregateFunction);
    }

    public <T> Table fromDataStream(DataStream<T> dataStream) {
        return this.streamTableEnvironment.fromDataStream(dataStream);
    }

    public <T> Table fromDataStream(DataStream<T> dataStream, String fields) {
        return this.streamTableEnvironment.fromDataStream(dataStream, fields);
    }

    public <T> Table fromDataStream(DataStream<T> dataStream, Expression... fields) {
        return this.streamTableEnvironment.fromDataStream(dataStream, fields);
    }

    public <T> void registerDataStream(String name, DataStream<T> dataStream) {
        this.streamTableEnvironment.registerDataStream(name, dataStream);
    }

    public <T> void createTemporaryView(String path, DataStream<T> dataStream) {
        this.streamTableEnvironment.createTemporaryView(path, dataStream);
    }

    public <T> void registerDataStream(String name, DataStream<T> dataStream, String fields) {
        this.registerDataStream(name, dataStream, fields);
    }

    public <T> void createTemporaryView(String path, DataStream<T> dataStream, String fields) {
        this.createTemporaryView(path, dataStream, fields);
    }

    public <T> void createTemporaryView(String path, DataStream<T> dataStream, Expression... fields) {
        this.createTemporaryView(path, dataStream, fields);
    }

    public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz) {
        return this.streamTableEnvironment.toAppendStream(table, clazz);
    }

    public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo) {
        return this.streamTableEnvironment.toAppendStream(table, typeInfo);
    }

    public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
        return this.streamTableEnvironment.toRetractStream(table, clazz);
    }

    public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, TypeInformation<T> typeInfo) {
        return this.streamTableEnvironment.toRetractStream(table, typeInfo);
    }

    public StreamTableDescriptor connect(ConnectorDescriptor connectorDescriptor) {
        return this.streamTableEnvironment.connect(connectorDescriptor);
    }

    public JobExecutionResult execute(String jobName) throws Exception {
        return this.streamTableEnvironment.execute(jobName);
    }

}
