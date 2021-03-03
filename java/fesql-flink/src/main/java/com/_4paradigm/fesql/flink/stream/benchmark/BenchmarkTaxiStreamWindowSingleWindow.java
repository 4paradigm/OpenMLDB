/*
 * java/fesql-flink/src/main/java/com/_4paradigm/fesql/flink/stream/benchmark/BenchmarkTaxiStreamWindowSingleWindow.java
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

package com._4paradigm.fesql.flink.stream.benchmark;

import com._4paradigm.fesql.flink.stream.FesqlStreamTableEnvironment;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import static org.apache.flink.table.api.Expressions.$;


public class BenchmarkTaxiStreamWindowSingleWindow {

	public static void main(String[] args) throws Exception {

		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		// Check params
		if (!params.has("input") || !params.has("output")) {
			System.err.println("Usage: --input <path> --output <path>");
			//return;
		}

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment sEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
		//FesqlStreamTableEnvironment sEnv = FesqlStreamTableEnvironment.create(sEnvOrigin);

		bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		bsEnv.setStateBackend(new MemoryStateBackend());

		// Input source
		String csvFilePath = null;
		if (params.has("input")) {
			csvFilePath = params.getRequired("input");
		}

		csvFilePath = "file:///Users/tobe/code/4pd/fesql/benchmark/window_offline_benchmark/taxi_tour_all_orderbytimestamp_csv/";

		CsvTableSource csvSrc = CsvTableSource.builder()
				.path(csvFilePath)
				.field("id", DataTypes.STRING())
				.field("vendor_id", DataTypes.INT())
				.field("passenger_count", DataTypes.INT())
				.field("pickup_longitude", DataTypes.DOUBLE())
				.field("pickup_latitude", DataTypes.DOUBLE())
				.field("dropoff_longitude", DataTypes.DOUBLE())
				.field("dropoff_latitude", DataTypes.DOUBLE())
				.field("store_and_fwd_flag", DataTypes.STRING())
				.field("trip_duration", DataTypes.INT())
				.field("pickup_datetime", DataTypes.BIGINT())
				.field("dropoff_datetime", DataTypes.BIGINT())
				.build();
		sEnv.registerTableSource("t2", csvSrc);

		// Output sink
		String csvOutputFile = null;
		if (params.has("output")) {
			csvOutputFile = params.getRequired("output");
		}

		csvOutputFile = "file:///tmp/flink_output/";

		CsvTableSink csvSink = new CsvTableSink(
				csvOutputFile,                  // output path
				",",                   // optional: delimit files by '|'
				1,                     // optional: write to a single file
				FileSystem.WriteMode.OVERWRITE);  // optional: override existing files

		sEnv.registerTableSink(
				"csvOutputTable",
				new String[]{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9",
						"c10", "c11", "c12", "c13"},
				new TypeInformation[]{
						org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.INT(), org.apache.flink.table.api.Types.DOUBLE(),
						org.apache.flink.table.api.Types.DOUBLE(), org.apache.flink.table.api.Types.DOUBLE(), org.apache.flink.table.api.Types.DOUBLE(),
						org.apache.flink.table.api.Types.DOUBLE(), org.apache.flink.table.api.Types.DOUBLE(),
						org.apache.flink.table.api.Types.DOUBLE(), org.apache.flink.table.api.Types.DOUBLE(),
						org.apache.flink.table.api.Types.DOUBLE(), org.apache.flink.table.api.Types.DOUBLE(), org.apache.flink.table.api.Types.DOUBLE(),
						org.apache.flink.table.api.Types.DOUBLE()},
				csvSink);

		DataStream<Row> inputCsvDatastream = sEnv.toAppendStream(sEnv.scan("t2"), Row.class);

		DataStream<Row> assignTimestampDatastream = inputCsvDatastream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>(){
			@Override
			public long extractTimestamp(Row row, long l) {
				return (long)row.getField(9);
			}

			@Nullable
			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(1000);
			}
		});

		Table assignTimestampTable = sEnv.fromDataStream(assignTimestampDatastream,
				$("id"),
				$("vendor_id"),
				$("passenger_count"),
				$("pickup_longitude"),
				$("pickup_latitude"),
				$("dropoff_longitude"),
				$("dropoff_latitude"),
				$("store_and_fwd_flag"),
				$("trip_duration"),
				$("pickup_datetime").rowtime(),
				$("dropoff_datetime"));

		sEnv.registerTable("t1", assignTimestampTable);

		String sqlText = "select \n" +
				"    trip_duration,\n" +
				"    passenger_count,\n" +
				"    sum(pickup_latitude) over w as vendor_sum_pl,\n" +
				"    max(pickup_latitude) over w as vendor_max_pl,\n" +
				"    min(pickup_latitude) over w as vendor_min_pl,\n" +
				"    avg(pickup_latitude) over w as vendor_avg_pl,\n" +
				"    sum(dropoff_latitude) over w as vendor_sum_pl2,\n" +
				"    max(dropoff_latitude) over w as vendor_max_pl2,\n" +
				"    min(dropoff_latitude) over w as vendor_min_pl2,\n" +
				"    avg(dropoff_latitude) over w as vendor_avg_pl2,\n" +
				"    sum(trip_duration) over w as vendor_sum_pl3,\n" +
				"    max(trip_duration) over w as vendor_max_pl3,\n" +
				"    min(trip_duration) over w as vendor_min_pl3,\n" +
				"    avg(trip_duration) over w as vendor_avg_pl3\n" +
				"from t1\n" +
				"window w as (partition by vendor_id order by pickup_datetime ROWS BETWEEN 10000 PRECEDING AND CURRENT ROW)";

		Table resultTable = sEnv.sqlQuery(sqlText);
		resultTable.executeInsert("csvOutputTable");

		//sEnv.toAppendStream(resultTable, Row.class).print();
		//bsEnv.execute("flink job");

	}

}
