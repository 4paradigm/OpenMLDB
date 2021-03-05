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

package com._4paradigm.fesql.flink.batch.planner;

import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.common.JITManager;
import com._4paradigm.fesql.common.SerializableByteBuffer;
import com._4paradigm.fesql.flink.common.*;
import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.node.ExprListNode;
import com._4paradigm.fesql.node.ExprNode;
import com._4paradigm.fesql.node.FrameType;
import com._4paradigm.fesql.node.OrderByNode;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class BatchWindowAggPlan {

    private static final Logger logger = LoggerFactory.getLogger(BatchWindowAggPlan.class);

    public static Table gen(GeneralPlanContext planContext, PhysicalWindowAggrerationNode node, Table childTable) throws FesqlException {

        DataSet<Row> inputDataset = planContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);

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
        Order order = Order.ASCENDING;
        if (!orderByNode.is_asc()) {
            order = Order.DESCENDING;
        }

        long startOffset = node.window().range().frame().GetHistoryRangeStart();
        long endOffset = node.window().range().frame().GetHistoryRangeEnd();
        long rowPreceding = -1 * node.window().range().frame().GetHistoryRowsStart();
        long maxSize = node.window().range().frame().frame_maxsize();
        boolean instanceNotInWindow = node.instance_not_in_window();
        boolean excludeCurrentTime = node.exclude_current_time();

        FrameType frameType = node.window().range().frame().frame_type();
        Window.WindowFrameType windowFrameType;
        if (frameType == FrameType.kFrameRows) {
            windowFrameType = Window.WindowFrameType.kFrameRows;
        } else if (frameType == FrameType.kFrameRowsMergeRowsRange) {
            windowFrameType = Window.WindowFrameType.kFrameRowsMergeRowsRange;
        } else {
            windowFrameType = Window.WindowFrameType.kFrameRowsRange;
        }
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

        DataSet<Row> outputDataset = inputDataset.groupBy(groupbyKeyIndexArray).sortGroup(orderbyKeyIndex, order).reduceGroup(new RichGroupReduceFunction<Row, Row>() {

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
            }

            @Override
            public void reduce(Iterable<Row> iterable, Collector<Row> collector) throws Exception {

                for (Row currentRow : iterable) {
                    com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(currentRow);

                    // TODO: Check type or orderby column
                    //LocalDateTime orderbyValue = (LocalDateTime)(currentRow.getField(orderbyKeyIndex));
                    //long orderbyLongValue = orderbyValue.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    Timestamp timestamp = (Timestamp) currentRow.getField(orderbyKeyIndex);
                    long orderbyLongValue = timestamp.getTime();

                    com._4paradigm.fesql.codec.Row outputFesqlRow = CoreAPI.WindowProject(functionPointer, orderbyLongValue, inputFesqlRow, true, appendSlices, windowInterface);

                    Row outputFlinkRow = outputCodec.decodeFesqlRow(outputFesqlRow);

                    collector.collect(outputFlinkRow);
                }

            }

            @Override
            public void close() throws Exception {
                super.close();
                inputCodec.delete();
                outputCodec.delete();
            }

        }).returns(finalOutputTypeInfo);

        return planContext.getBatchTableEnvironment().fromDataSet(outputDataset);

    }

}
