package com._4paradigm.fesql.flink.stream.benchmark;

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
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import javax.annotation.Nullable;
import static org.apache.flink.table.api.Expressions.$;


public class StandardFlinkOverWindow {

	public static void main(String[] args) throws Exception {

		final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment sEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

		bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		bsEnv.setStateBackend(new MemoryStateBackend());

		// Check params
		if (!params.has("input") || !params.has("output")) {
			System.err.println("Usage: --input <path> --output <path>");
			return;
		}

		// Input source
		String csvFilePath = null;
		if (params.has("input")) {
			csvFilePath = params.getRequired("input");
		} else {
			throw new Exception("Need to set input csv path");
		}

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
				.field("pickup_datetime_long", DataTypes.BIGINT())
				.field("dropoff_datetime_long", DataTypes.BIGINT())
				.build();
		sEnv.registerTableSource("t1", csvSrc);

		// Output sink
		String csvOutputFile = null;
		if (params.has("output")) {
			csvOutputFile = params.getRequired("output");
		}
		CsvTableSink csvSink = new CsvTableSink(
				csvOutputFile,                  // output path
				",",                   // optional: delimit files by '|'
				1,                     // optional: write to a single file
				FileSystem.WriteMode.OVERWRITE);  // optional: override existing files
		sEnv.registerTableSink(
			"csvOutputTable",
			// specify table schema
			new String[]{"c1", "c2", "c3", "c4", "c5"},
			new TypeInformation[]{Types.STRING(), Types.INT(), Types.INT(),
					Types.INT(), Types.INT()},
			csvSink);

		DataStream<Row> inputCsvDatastream = sEnv.toAppendStream(sEnv.sqlQuery("select * from t1"), Row.class);

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
				$("pickup_datetime_long").rowtime(),
				$("dropoff_datetime_long"));

		sEnv.registerTable("t2", assignTimestampTable);

		String sqlText = "select id, vendor_id, " +
				"sum(passenger_count) over w as vendor_sum_pl, " +
				"max(passenger_count) over w as vendor_max_pl, " +
				"min(passenger_count) over w as vendor_min_pl " +
				"from t2 " +
				"window w as (partition by vendor_id order by pickup_datetime_long ROWS BETWEEN 10000 PRECEDING AND CURRENT ROW)";

		Table resultTable = sEnv.sqlQuery(sqlText);
		resultTable.executeInsert("csvOutputTable");
		//sEnv.toAppendStream(resultTable, Row.class).print();
		//bsEnv.execute("flink job");

	}

}
