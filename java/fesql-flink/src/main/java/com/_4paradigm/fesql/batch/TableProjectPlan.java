package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.common.*;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.CoreAPI;
import com._4paradigm.fesql.vm.FeSQLJITWrapper;
import com._4paradigm.fesql.vm.PhysicalTableProjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class TableProjectPlan {

    private static Logger logger = LoggerFactory.getLogger(TableProjectPlan.class);

    public static Table gen(BatchPlanContext batchPlanContext, PhysicalTableProjectNode node, Table childTable) {

        DataSet<Row> inputDataset = batchPlanContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);

        // Take out the serializable objects
        String functionName = node.project().fn_name();
        String moduleTag = batchPlanContext.getTag();
        SerializableByteBuffer moduleBuffer = batchPlanContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = FesqlUtil.getMergedNodeOutputColumnList(node);
        RowTypeInfo finalOutputTypeInfo = null;
        try {
            finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);
        } catch (FesqlException e) {
            e.printStackTrace();
            logger.error("Fail to generate Flink row type info, error message: " + e.getMessage());
        }

        DataSet<Row> outputDataset = inputDataset.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                // Init in executors with serializable objects
                ByteBuffer moduleBroadcast = moduleBuffer.getBuffer();
                JITManager.initJITModule(moduleTag, moduleBroadcast);
                FeSQLJITWrapper jit = JITManager.getJIT(moduleTag);
                long functionPointer = jit.FindFunction(functionName);
                FesqlFlinkCodec inputCodec = new FesqlFlinkCodec(inputSchemaLists);
                FesqlFlinkCodec outputCodec = new FesqlFlinkCodec(outputSchemaLists);

                // Encode, run and decode
                com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(row);
                com._4paradigm.fesql.codec.Row outputNativeRow = CoreAPI.RowProject(functionPointer, inputFesqlRow, false);
                Row flinkRow = outputCodec.decodeFesqlRow(outputNativeRow);

                return flinkRow;
            }
        }).returns(finalOutputTypeInfo);

        // Convert DataSet<Row> to Table
        return batchPlanContext.getBatchTableEnvironment().fromDataSet(outputDataset);
    }

}
