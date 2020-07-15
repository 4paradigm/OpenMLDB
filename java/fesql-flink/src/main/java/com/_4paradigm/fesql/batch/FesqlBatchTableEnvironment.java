package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.common.FesqlException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FesqlBatchTableEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FesqlBatchTableEnvironment.class);

    private BatchTableEnvironment batchTableEnvironment;

    private Map<String, TableSchema> registeredTableSchemaMap = new HashMap<String, TableSchema>();

    public FesqlBatchTableEnvironment(BatchTableEnvironment batchTableEnvironment) {
        this.batchTableEnvironment = batchTableEnvironment;
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return this.batchTableEnvironment;
    }

    public Map<String, TableSchema> getRegisteredTableSchemaMap() {
        return this.registeredTableSchemaMap;
    }

    public void registerTableSource(String name, TableSource<?> tableSource) {
        this.batchTableEnvironment.registerTableSource(name, tableSource);

        // Register table name and schema
        if (this.registeredTableSchemaMap.containsKey(name)) {
            logger.warn(String.format("The table %s has been registered, ignore registeration", name));
        } else {
            this.registeredTableSchemaMap.put(name, tableSource.getTableSchema());
        }
    }

    public void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {
        this.batchTableEnvironment.registerTableSink(name, fieldNames, fieldTypes, tableSink);
    }

    public void registerTableSink(String name, TableSink<?> configuredSink) {
        this.batchTableEnvironment.registerTableSink(name, configuredSink);
    }

    public FesqlTable sqlQuery(String query) {
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
                }
            }
        }
        return null;
    }

    public FesqlTable fesqlQuery(String query) {
        try {
            return runFesqlQuery(query);
        } catch (Exception e) {
            logger.warn("Fail to execute with FESQL, error message: " + e.getMessage());
            return flinksqlQuery(query);
        }
    }

    public FesqlTable runFesqlQuery(String query) throws Exception {
        // Normalize SQL format
        if (!query.trim().endsWith(";")) {
            query = query.trim() + ";";
        }

        FesqlBatchPlanner planner = new FesqlBatchPlanner(this);
        return new FesqlTable(planner.plan(query));
    }

    public FesqlTable flinksqlQuery(String query) {
        return new FesqlTable(this.batchTableEnvironment.sqlQuery(query));
    }

    public FesqlTable fromDataSet(DataSet<Row> dataSet) {
        return new FesqlTable(this.batchTableEnvironment.fromDataSet(dataSet));
    }

    public FesqlTable fromDataSet(DataSet<Row> dataSet, String fields) {
        return new FesqlTable(this.batchTableEnvironment.fromDataSet(dataSet, fields));
    }

    public FesqlTable fromDataSet(DataSet<Row> dataSet, Expression... fields) {
        return new FesqlTable(this.batchTableEnvironment.fromDataSet(dataSet, fields));
    }

    public <T> DataSet<T> toDataSet(FesqlTable table, Class<T> clazz) {
        return this.batchTableEnvironment.toDataSet(table.getTable(), clazz);
    }

    public <T> DataSet<T> toDataSet(FesqlTable table, TypeInformation<T> typeInfo) {
        return this.batchTableEnvironment.toDataSet(table.getTable(), typeInfo);
    }

}
