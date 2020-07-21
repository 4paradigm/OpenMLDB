package com._4paradigm.fesql.stream;

import com._4paradigm.fesql.common.FesqlPlanner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
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

    public void registerTable(String name, Table table) {
        this.streamTableEnvironment.registerTable(name, table);

        // Register table name and schema
        if (this.registeredTableSchemaMap.containsKey(name)) {
            logger.warn(String.format("The table %s has been registered, ignore registeration", name));
        } else {
            this.registeredTableSchemaMap.put(name, table.getSchema());
        }
    }

    public void registerTableSource(String name, TableSource<?> tableSource) {
        this.streamTableEnvironment.registerTableSource(name, tableSource);

        // Register table name and schema
        if (this.registeredTableSchemaMap.containsKey(name)) {
            logger.warn(String.format("The table %s has been registered, ignore registeration", name));
        } else {
            this.registeredTableSchemaMap.put(name, tableSource.getTableSchema());
        }
    }

    public void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {
        this.streamTableEnvironment.registerTableSink(name, fieldNames, fieldTypes, tableSink);
    }

    public void registerTableSink(String name, TableSink<?> configuredSink) {
        this.registerTableSink(name, configuredSink);
    }


    public Table sqlQuery(String query) {
        String isDisableFesql = System.getenv("DISABLE_FESQL");
        if (isDisableFesql != null && isDisableFesql.trim().toLowerCase().equals("true")) {
            // Force to run FlinkSQL
            return flinksqlQuery(query);
        } else {
            try {
                // Try to run FESQL
                return runFesqlQuery(query);
            } catch (Exception e) {
                String isEnableFesqlFallback = System.getenv("ENABLE_FESQL_FALLBACK");
                if (isEnableFesqlFallback != null && isEnableFesqlFallback.trim().toLowerCase().equals("true")) {
                    // Fallback to FlinkSQL
                    logger.warn("Fail to execute with FESQL, fallback to FlinkSQL");
                    return flinksqlQuery(query);
                } else {
                    logger.error("Fail to execute with FESQL, error message: " + e.getMessage());
                }
            }
        }
        return null;
    }

    public Table fesqlQuery(String query) {
        try {
            return runFesqlQuery(query);
        } catch (Exception e) {
            logger.warn("Fail to execute with FESQL, error message: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public Table runFesqlQuery(String query) throws Exception {
        // Normalize SQL format
        if (!query.trim().endsWith(";")) {
            query = query.trim() + ";";
        }

        FesqlPlanner planner = new FesqlPlanner(this);
        return planner.plan(query);
    }

    public Table flinksqlQuery(String query) {
        return this.streamTableEnvironment.sqlQuery(query);
    }

    public TableResult executeSql(String statement) {
        // TODO: Use the Flink implementation by default
        return this.streamTableEnvironment.executeSql(statement);
    }

}
