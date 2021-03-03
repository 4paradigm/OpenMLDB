/*
 * TestStreamLimitPlan.java
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

package com._4paradigm.fesql.flink.stream.planner;

import com._4paradigm.fesql.flink.common.ParquetHelper;
import com._4paradigm.fesql.flink.stream.FesqlStreamTableEnvironment;
import org.apache.flink.formats.parquet.ParquetTableSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;


public class TestStreamLimitPlan {

    @Test
    public void testStreamLimitPlan() throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment sEnvOrigin = StreamTableEnvironment.create(bsEnv, bsSettings);
        FesqlStreamTableEnvironment sEnv = FesqlStreamTableEnvironment.create(sEnvOrigin);

        // Input source
        String csvFilePath = this.getClass().getResource("/int_int_int.csv").getPath();
        CsvTableSource csvSrc = CsvTableSource.builder()
                .path(csvFilePath)
                .field("vendor_id", DataTypes.INT())
                .field("passenger_count", DataTypes.INT())
                .field("trip_duration", DataTypes.INT())
                .build();
        sEnv.registerTableSource("t1", csvSrc);

        // Run sql
        String sqlText = "select vendor_id + 1000, passenger_count * 10, trip_duration - 10 from t1 limit 10";
        Table table = sEnv.fesqlQuery(sqlText);

        // Check running without exception
        sEnv.toAppendStream(table, Row.class).print();
        bsEnv.execute();
    }

}
