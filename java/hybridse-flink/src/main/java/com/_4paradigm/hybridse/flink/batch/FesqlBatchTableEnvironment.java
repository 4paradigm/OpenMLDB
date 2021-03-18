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

package com._4paradigm.hybridse.flink.batch;

import com._4paradigm.hybridse.common.FesqlException;
import com._4paradigm.hybridse.common.UnsupportedFesqlException;
import com._4paradigm.hybridse.flink.common.planner.FesqlFlinkPlanner;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
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

    public static FesqlBatchTableEnvironment create(BatchTableEnvironment batchTableEnvironment) {
        return new FesqlBatchTableEnvironment(batchTableEnvironment);
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return this.batchTableEnvironment;
    }

    public Map<String, TableSchema> getRegisteredTableSchemaMap() {
        return this.registeredTableSchemaMap;
    }

    public void registerTable(String name, Table table) {
        this.batchTableEnvironment.registerTable(name, table);
        if (this.registeredTableSchemaMap.containsKey(name)) {
            logger.warn(String.format("The table %s has been registered, ignore registeration", name));
        } else {
            this.registeredTableSchemaMap.put(name, table.getSchema());
        }
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

    public Table sqlQuery(String query) {
        try {
            return fesqlQuery(query);
        } catch (Exception e) {
            logger.warn("Fail to execute with FESQL, error message: {}", e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public Table fesqlQuery(String query) throws FesqlException, UnsupportedFesqlException {
        // Normalize SQL format
        if (!query.trim().endsWith(";")) {
            query = query.trim() + ";";
        }

        FesqlFlinkPlanner planner = new FesqlFlinkPlanner(this);
        return planner.plan(query);
    }

    public Table flinksqlQuery(String query) {
        return this.batchTableEnvironment.sqlQuery(query);
    }

    public Table fromDataSet(DataSet<Row> dataSet) {
        return this.batchTableEnvironment.fromDataSet(dataSet);
    }

    public Table fromDataSet(DataSet<Row> dataSet, String fields) {
        return this.batchTableEnvironment.fromDataSet(dataSet, fields);
    }

    public Table fromDataSet(DataSet<Row> dataSet, Expression... fields) {
        return this.batchTableEnvironment.fromDataSet(dataSet, fields);
    }

    public <T> DataSet<T> toDataSet(Table table, Class<T> clazz) {
        return this.batchTableEnvironment.toDataSet(table, clazz);
    }

    public <T> DataSet<T> toDataSet(Table table, TypeInformation<T> typeInfo) {
        return this.batchTableEnvironment.toDataSet(table, typeInfo);
    }

    public JobExecutionResult execute(String jobName) throws Exception {
        return this.batchTableEnvironment.execute(jobName);
    }

}
