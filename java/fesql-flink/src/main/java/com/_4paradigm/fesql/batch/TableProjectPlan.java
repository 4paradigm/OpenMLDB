package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.common.*;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.CoreAPI;
import com._4paradigm.fesql.vm.FeSQLJITWrapper;
import com._4paradigm.fesql.vm.PhysicalTableProjectNode;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;

public class TableProjectPlan {

    private static final Logger logger = LoggerFactory.getLogger(TableProjectPlan.class);

    public static Table gen(FesqlPlanContext batchPlanContext, PhysicalTableProjectNode node, Table childTable) throws FesqlException {

        DataSet<Row> inputDataset = batchPlanContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);

        // Take out the serializable objects
        String functionName = node.project().fn_name();
        String moduleTag = batchPlanContext.getTag();
        SerializableByteBuffer moduleBuffer = batchPlanContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = FesqlUtil.getMergedNodeOutputColumnList(node);
        RowTypeInfo finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);

        DataSet<Row> outputDataset = inputDataset.mapPartition(new MapPartitionFunction<Row, Row>() {

            @Override
            public void mapPartition(Iterable<Row> iterable, Collector<Row> collector) throws Exception {
                // Init in executors with serializable objects
                ByteBuffer moduleByteBuffer = moduleBuffer.getBuffer();
                JITManager.initJITModule(moduleTag, moduleByteBuffer);
                FeSQLJITWrapper jit = JITManager.getJIT(moduleTag);
                long functionPointer = jit.FindFunction(functionName);

                FesqlFlinkCodec inputCodec = new FesqlFlinkCodec(inputSchemaLists);
                FesqlFlinkCodec outputCodec = new FesqlFlinkCodec(outputSchemaLists);

                for (Row inputRow: iterable) {
                    com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(inputRow);
                    com._4paradigm.fesql.codec.Row outputNativeRow = CoreAPI.RowProject(functionPointer, inputFesqlRow, false);
                    Row flinkRow = outputCodec.decodeFesqlRow(outputNativeRow);
                    collector.collect(flinkRow);
                }
            }
        }).returns(finalOutputTypeInfo);

        // Convert DataSet<Row> to Table
        return batchPlanContext.getBatchTableEnvironment().fromDataSet(outputDataset);
    }

}
