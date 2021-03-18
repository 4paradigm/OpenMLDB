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

package com._4paradigm.hybridse.flink.stream.planner;

import com._4paradigm.hybridse.common.FesqlException;
import com._4paradigm.hybridse.common.JITManager;
import com._4paradigm.hybridse.common.SerializableByteBuffer;
import com._4paradigm.hybridse.flink.common.*;
import com._4paradigm.hybridse.flink.common.planner.GeneralPlanContext;
import com._4paradigm.hybridse.node.ExprListNode;
import com._4paradigm.hybridse.node.ExprNode;
import com._4paradigm.hybridse.node.FrameType;
import com._4paradigm.hybridse.node.OrderByNode;
import com._4paradigm.hybridse.type.TypeOuterClass;
import com._4paradigm.hybridse.vm.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;


public class StreamWindowAggPlan {

    private static final Logger logger = LoggerFactory.getLogger(StreamWindowAggPlan.class);

    public static Table gen(GeneralPlanContext planContext, PhysicalWindowAggrerationNode node, Table childTable) throws FesqlException {

        DataStream<Row> inputDatastream = planContext.getStreamTableEnvironment().toAppendStream(childTable, Row.class);

        // Take out the serializable objects
        String functionName = node.project().fn_info().fn_name();
        String moduleTag = planContext.getTag();
        SerializableByteBuffer moduleBuffer = planContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = node.GetOutputSchema();
        RowTypeInfo finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);

        WindowOp windowOp = node.window();

        // Get group-by info
        ExprListNode groupbyKeyExprs = windowOp.partition().keys();
        List<Integer> groupbyKeyIndexes = new ArrayList<Integer>();
        for (int i = 0; i < groupbyKeyExprs.GetChildNum(); ++i) {
            ExprNode exprNode = groupbyKeyExprs.GetChild(i);
            int index = FesqlUtil.resolveColumnIndex(exprNode, node.GetProducer(0));
            groupbyKeyIndexes.add(index);
        }

        // Get order-by info
        OrderByNode orderByNode = windowOp.sort().orders();
        ExprListNode orderbyExprListNode = orderByNode.order_by();
        if (orderbyExprListNode.GetChildNum() > 1) {
            throw new FesqlException("Multiple window order is not supported yet");
        }
        ExprNode orderbyExprNode = orderbyExprListNode.GetChild(0);
        int orderbyKeyIndex = FesqlUtil.resolveColumnIndex(orderbyExprNode, node.GetProducer(0));
        // TODO: Use timer to trigger instead of sorting, do not support descending now
        if (!orderByNode.is_asc()) {
            throw new FesqlException("Do not support desceding for over window");
        }

        FrameType frameType = node.window().range().frame().frame_type();
        Window.WindowFrameType windowFrameType;
        if (frameType == FrameType.kFrameRows) {
            windowFrameType = Window.WindowFrameType.kFrameRows;
        } else if (frameType == FrameType.kFrameRowsMergeRowsRange) {
            windowFrameType = Window.WindowFrameType.kFrameRowsMergeRowsRange;
        } else {
            windowFrameType = Window.WindowFrameType.kFrameRowsRange;
        }

        long startOffset = node.window().range().frame().GetHistoryRangeStart();
        long endOffset = node.window().range().frame().GetHistoryRangeEnd();
        long rowPreceding = -1 * node.window().range().frame().GetHistoryRowsStart();
        long maxSize = node.window().range().frame().frame_maxsize();
        boolean instanceNotInWindow = node.instance_not_in_window();
        boolean excludeCurrentTime = node.exclude_current_time();
        boolean needAppendInput = node.need_append_input();
        int appendSlices;
        if (needAppendInput) {
            appendSlices = inputSchemaLists.size();
        } else {
            appendSlices = 0;
        }

        // Parse List<Integer> to int[] to use as group by
        int groupbyKeySize = groupbyKeyIndexes.size();
        int[] groupbyKeyIndexArray = new int[groupbyKeySize];
        for (int i = 0; i < groupbyKeySize; ++i) {
            groupbyKeyIndexArray[i] = groupbyKeyIndexes.get(i);
        }

        DataStream<Row> outputDatastream = null;

        // Check if event time or process time
        TimeCharacteristic timeCharacteristic = ((StreamTableEnvironmentImpl) planContext.getStreamTableEnvironment()).execEnv().getStreamTimeCharacteristic();

        if (timeCharacteristic.equals(TimeCharacteristic.EventTime)) {

            outputDatastream = inputDatastream.keyBy(groupbyKeyIndexArray).process(new KeyedProcessFunction<Tuple, Row, Row>() {

                long functionPointer;
                FesqlFlinkCodec inputCodec;
                FesqlFlinkCodec outputCodec;
                WindowInterface windowInterface;

                private ValueState<Long> lastTriggeringTsState;
                private MapState<Long, List<Row>> timeRowsState;

                @Override
                public void open(Configuration config) throws Exception {
                    super.open(config);

                    // Init non-serializable objects
                    ByteBuffer moduleByteBuffer = moduleBuffer.getBuffer();
                    JITManager.initJITModule(moduleTag, moduleByteBuffer);
                    FeSQLJITWrapper jit = JITManager.getJIT(moduleTag);
                    functionPointer = jit.FindFunction(functionName);
                    inputCodec = new FesqlFlinkCodec(inputSchemaLists);
                    outputCodec = new FesqlFlinkCodec(outputSchemaLists);
                    windowInterface = new WindowInterface(instanceNotInWindow, excludeCurrentTime, windowFrameType.toString(), startOffset, endOffset, rowPreceding, maxSize);


                    // Init state
                    ValueStateDescriptor<Long> lastTriggeringTsDescriptor = new ValueStateDescriptor<>("lastTriggeringTsState", Long.class);
                    lastTriggeringTsState = this.getRuntimeContext().getState(lastTriggeringTsDescriptor);

                    TypeInformation<Long> keyTypeInformation = BasicTypeInfo.LONG_TYPE_INFO;
                    TypeInformation<List<Row>> valueTypeInformation = new ListTypeInfo<Row>(Row.class);
                    MapStateDescriptor<Long, List<Row>> mapStateDescriptor = new MapStateDescriptor<Long, List<Row>>("timeRowsState", keyTypeInformation, valueTypeInformation);
                    timeRowsState = this.getRuntimeContext().getMapState(mapStateDescriptor);
                }

                @Override
                public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {

                    // TODO: Check type or orderby column
                    LocalDateTime orderbyValue = (LocalDateTime) row.getField(orderbyKeyIndex);
                    long orderbyLongValue = orderbyValue.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

                    Long lastTriggeringTs = this.lastTriggeringTsState.value();

                    if (lastTriggeringTs == null || orderbyLongValue > lastTriggeringTs) { // Handle if timestamp is not out-of-date
                        List<Row> data = this.timeRowsState.get(orderbyLongValue);
                        if (data == null) { // Register timer if the timestamp is new
                            List<Row> rows = new ArrayList<Row>();
                            rows.add(row);
                            this.timeRowsState.put(orderbyLongValue, rows);
                            // TODO: Only support for event time now
                            context.timerService().registerEventTimeTimer(orderbyLongValue);
                        } else { // Add the row to state
                            data.add(row);
                            this.timeRowsState.put(orderbyLongValue, data);
                        }
                    }
                }

                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<Tuple, Row, Row>.OnTimerContext ctx, Collector<Row> out) throws Exception {
                    List<Row> inputs = this.timeRowsState.get(timestamp);

                    if (inputs != null) {
                        for (Row inputRow : inputs) {
                            com._4paradigm.hybridse.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(inputRow);
                            com._4paradigm.hybridse.codec.Row outputFesqlRow = CoreAPI.WindowProject(functionPointer, timestamp, inputFesqlRow, true, appendSlices, windowInterface);
                            Row outputFlinkRow = outputCodec.decodeFesqlRow(outputFesqlRow);
                            out.collect(outputFlinkRow);
                        }
                    }

                    // Clear state if the data has been processed
                    this.timeRowsState.remove(timestamp);
                }

                @Override
                public void close() throws Exception {
                    super.close();
                    inputCodec.delete();
                    outputCodec.delete();
                }

            }).returns(finalOutputTypeInfo);

        } else { // Handle process time

            outputDatastream = inputDatastream.keyBy(groupbyKeyIndexArray).process(new KeyedProcessFunction<Tuple, Row, Row>() {

                long functionPointer;
                FesqlFlinkCodec inputCodec;
                FesqlFlinkCodec outputCodec;
                WindowInterface windowInterface;

                @Override
                public void open(Configuration config) throws Exception {
                    super.open(config);

                    // Init non-serializable objects
                    ByteBuffer moduleByteBuffer = moduleBuffer.getBuffer();
                    JITManager.initJITModule(moduleTag, moduleByteBuffer);
                    FeSQLJITWrapper jit = JITManager.getJIT(moduleTag);
                    functionPointer = jit.FindFunction(functionName);
                    inputCodec = new FesqlFlinkCodec(inputSchemaLists);
                    outputCodec = new FesqlFlinkCodec(outputSchemaLists);
                    windowInterface = new WindowInterface(instanceNotInWindow, excludeCurrentTime, windowFrameType.toString(), startOffset, endOffset, rowPreceding, maxSize);

                    // Init state
                    TypeInformation<Long> keyTypeInformation = BasicTypeInfo.LONG_TYPE_INFO;
                    TypeInformation<List<Row>> valueTypeInformation = new ListTypeInfo<Row>(Row.class);
                    MapStateDescriptor<Long, List<Row>> mapStateDescriptor = new MapStateDescriptor<Long, List<Row>>("timeRowsState", keyTypeInformation, valueTypeInformation);
                }

                @Override
                public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {

                    // TODO: Check type or orderby column
                    LocalDateTime orderbyValue = (LocalDateTime) row.getField(orderbyKeyIndex);
                    long orderbyLongValue = orderbyValue.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

                    com._4paradigm.hybridse.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(row);
                    com._4paradigm.hybridse.codec.Row outputFesqlRow = CoreAPI.WindowProject(functionPointer, orderbyLongValue, inputFesqlRow, true, appendSlices, windowInterface);
                    Row outputFlinkRow = outputCodec.decodeFesqlRow(outputFesqlRow);
                    collector.collect(outputFlinkRow);
                }

                @Override
                public void onTimer(long timestamp, KeyedProcessFunction<Tuple, Row, Row>.OnTimerContext ctx, Collector<Row> out) throws Exception {
                    super.onTimer(timestamp, ctx, out);
                }

                @Override
                public void close() throws Exception {
                    super.close();
                    inputCodec.delete();
                    outputCodec.delete();
                }

            }).returns(finalOutputTypeInfo);

        }

        // Convert DataStream<Row> to Table
        return planContext.getStreamTableEnvironment().fromDataStream(outputDatastream);

    }

}
