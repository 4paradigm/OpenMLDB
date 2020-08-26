package com._4paradigm.fesql.flink.batch.planner;

import com._4paradigm.fesql.flink.batch.FesqlBatchTableEnvironment;
import com._4paradigm.fesql.flink.common.ParquetHelper;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.formats.parquet.ParquetTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class TestBatchLimitPlan {

    @Test
    public void testBatchLimitPlan() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment originEnv = BatchTableEnvironment.create(env);
        FesqlBatchTableEnvironment tEnv = new FesqlBatchTableEnvironment(originEnv);

        // Init input source
        String parquetFilePath = this.getClass().getResource("/int_int_int.parquet").getPath();
        MessageType messageType = ParquetHelper.readParquetSchema(parquetFilePath);
        ParquetTableSource parquetSrc = ParquetTableSource.builder()
                .path(parquetFilePath)
                .forParquetSchema(messageType)
                .build();
        tEnv.registerTableSource("t1", parquetSrc);

        // Run sql
        String sqlText = "select vendor_id, passenger_count, trip_duration from t1 limit 10";
        Table table = tEnv.fesqlQuery(sqlText);

        // Check result
        List<Row> fesqlResult = tEnv.toDataSet(table, Row.class).collect();
        assertEquals(10, fesqlResult.size());
    }

}
