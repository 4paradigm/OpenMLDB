package com._4paradigm.fesql.stream.planner;

import com._4paradigm.fesql.common.*;
import com._4paradigm.fesql.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.node.ExprListNode;
import com._4paradigm.fesql.node.ExprNode;
import com._4paradigm.fesql.node.OrderByNode;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
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
        String functionName = node.project().fn_name();
        String moduleTag = planContext.getTag();
        SerializableByteBuffer moduleBuffer = planContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = FesqlUtil.getMergedNodeOutputColumnList(node);
        RowTypeInfo finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);

        WindowOp windowOp = node.window();

        // Get group-by info
        ExprListNode groupbyKeyExprs = windowOp.partition().keys();
        List<Integer> groupbyKeyIndexes = new ArrayList<Integer>();
        for (int i=0; i < groupbyKeyExprs.GetChildNum(); ++i) {
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

        long startOffset = node.window().range().frame().GetHistoryRangeStart();
        long rowPreceding = -1 * node.window().range().frame().GetHistoryRowsStart();
        boolean instanceNotInWindow = node.instance_not_in_window();
        boolean needAppendInput = node.need_append_input();
        int appendSlices;
        if (needAppendInput) {
            appendSlices = inputSchemaLists.size();
        } else {
            appendSlices = 0;
        }

        // TODO: keyby multiple keys
        DataStream<Row> outputDatastream = inputDatastream.keyBy(groupbyKeyIndexes.get(0)).process(new KeyedProcessFunction<Tuple, Row, Row>() {

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
                windowInterface = new WindowInterface(instanceNotInWindow, startOffset, 0, rowPreceding, 0);
            }

            @Override
            public void processElement(Row row, Context context, Collector<Row> collector) throws Exception {
                processWindowCompute(row, collector);
            }

            public void processWindowCompute(Row currentRow, Collector<Row> collector) throws Exception {
                com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(currentRow);

                // TODO: Check type or orderby column
                LocalDateTime orderbyValue = (LocalDateTime)(currentRow.getField(orderbyKeyIndex));
                long orderbyLongValue = orderbyValue.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

                com._4paradigm.fesql.codec.Row outputFesqlRow = CoreAPI.WindowProject(functionPointer, orderbyLongValue, inputFesqlRow, true, appendSlices, windowInterface);

                Row outputFlinkRow = outputCodec.decodeFesqlRow(outputFesqlRow);

                collector.collect(outputFlinkRow);
            }

        }).returns(finalOutputTypeInfo);

        // Convert DataStream<Row> to Table
        return planContext.getStreamTableEnvironment().fromDataStream(outputDatastream);

    }

}
