package com._4paradigm.fesql.batch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class FesqlBatchTableEnvironment {

    private BatchTableEnvironment batchTableEnvironment;

    public FesqlBatchTableEnvironment(BatchTableEnvironment batchTableEnvironment) {
        this.batchTableEnvironment = batchTableEnvironment;
    }

    BatchTableEnvironment getBatchTableEnvironment() {
        return this.batchTableEnvironment;
    }

    void registerTableSource(String name, TableSource<?> tableSource) {
        this.batchTableEnvironment.registerTableSource(name, tableSource);

        // TODO: Get table name and schema in map
    }

    void registerTableSink(String name, String[] fieldNames, TypeInformation<?>[] fieldTypes, TableSink<?> tableSink) {
        this.batchTableEnvironment.registerTableSink(name, fieldNames, fieldTypes, tableSink);
    }

    void registerTableSink(String name, TableSink<?> configuredSink) {
        this.batchTableEnvironment.registerTableSink(name, configuredSink);
    }

    FesqlTable sqlQuery(String query) {
        // TODO: Change to FESQL execution engine
        return new FesqlTable(this.batchTableEnvironment.sqlQuery(query));
    }

    FesqlTable flinksqlQuery(String query) {
        return new FesqlTable(this.batchTableEnvironment.sqlQuery(query));
    }

    FesqlTable fromDataSet(DataSet<Row> dataSet) {
        return new FesqlTable(this.batchTableEnvironment.fromDataSet(dataSet));
    }

    FesqlTable fromDataSet(DataSet<Row> dataSet, String fields) {
        return new FesqlTable(this.batchTableEnvironment.fromDataSet(dataSet, fields));
    }

    FesqlTable fromDataSet(DataSet<Row> dataSet, Expression... fields) {
        return new FesqlTable(this.batchTableEnvironment.fromDataSet(dataSet, fields));
    }

}
