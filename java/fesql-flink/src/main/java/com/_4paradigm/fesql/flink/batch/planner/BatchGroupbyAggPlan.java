package com._4paradigm.fesql.flink.batch.planner;

import com._4paradigm.fesql.flink.common.*;
import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.node.ExprListNode;
import com._4paradigm.fesql.node.ExprNode;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class BatchGroupbyAggPlan {

    private static final Logger logger = LoggerFactory.getLogger(BatchGroupbyAggPlan.class);

    public static Table gen(GeneralPlanContext planContext, PhysicalGroupAggrerationNode node, Table childTable) throws FesqlException {

        DataSet<Row> inputDataset = planContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);

        // Take out the serializable objects
        String functionName = node.project().fn_name();
        String moduleTag = planContext.getTag();
        SerializableByteBuffer moduleBuffer = planContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<TypeOuterClass.ColumnDef> inputSchema = FesqlUtil.getMergedNodeOutputColumnList(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = FesqlUtil.getMergedNodeOutputColumnList(node);
        RowTypeInfo finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);

        // Get group-by info
        ExprListNode groupbyKeyExprs = node.getGroup_().keys();
        List<Integer> groupbyKeyIndexes = new ArrayList<Integer>();
        for (int i=0; i < groupbyKeyExprs.GetChildNum(); ++i) {
            ExprNode exprNode = groupbyKeyExprs.GetChild(i);
            int index = FesqlUtil.resolveColumnIndex(exprNode, node.GetProducer(0));
            groupbyKeyIndexes.add(index);
        }

        // Parse List<Integer> to int[] to use as group by
        int groupbyKeySize = groupbyKeyIndexes.size();
        int[] groupbyKeyIndexArray = new int[groupbyKeySize];
        for (int i = 0; i < groupbyKeySize; ++i) {
            groupbyKeyIndexArray[i] = groupbyKeyIndexes.get(i);
        }

        DataSet<Row> outputDataset = inputDataset.groupBy(groupbyKeyIndexArray).reduceGroup(new RichGroupReduceFunction<Row, Row>() {

            long functionPointer;
            FesqlFlinkCodec inputCodec;
            FesqlFlinkCodec outputCodec;
            GroupbyInterface groupbyInterface;
            ArrayList<com._4paradigm.fesql.codec.Row> bufferFesqlRows = new ArrayList<>();

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
                groupbyInterface = new GroupbyInterface(inputSchema);
            }

            @Override
            public void reduce(Iterable<Row> iterable, Collector<Row> collector) throws Exception {

                for (Row currentRow: iterable) {
                    com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(currentRow);
                    groupbyInterface.AddRow(inputFesqlRow);
                    bufferFesqlRows.add(inputFesqlRow);
                }

                com._4paradigm.fesql.codec.Row outputFesqlRow = CoreAPI.GroupbyProject(functionPointer, groupbyInterface);
                Row outputFlinkRow = outputCodec.decodeFesqlRow(outputFesqlRow);
                collector.collect(outputFlinkRow);
            }

            @Override
            public void close() throws Exception {
                super.close();
                inputCodec.delete();
                outputCodec.delete();
                for (com._4paradigm.fesql.codec.Row fesqlRow: bufferFesqlRows) {
                    fesqlRow.delete();
                }
                groupbyInterface.delete();
            }

        }).returns(finalOutputTypeInfo);

        return planContext.getBatchTableEnvironment().fromDataSet(outputDataset);

    }

}
