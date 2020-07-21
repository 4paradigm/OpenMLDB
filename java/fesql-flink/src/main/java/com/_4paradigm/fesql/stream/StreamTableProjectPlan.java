package com._4paradigm.fesql.stream;

import com._4paradigm.fesql.common.*;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.CoreAPI;
import com._4paradigm.fesql.vm.FeSQLJITWrapper;
import com._4paradigm.fesql.vm.PhysicalTableProjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class StreamTableProjectPlan {

    private static final Logger logger = LoggerFactory.getLogger(StreamTableProjectPlan.class);

    public static Table gen(FesqlPlanContext planContext, PhysicalTableProjectNode node, Table childTable) throws FesqlException {

        DataStream<Row> inputDatastream = planContext.getStreamTableEnvironment().toAppendStream(childTable, Row.class);

        // Take out the serializable objects
        String functionName = node.project().fn_name();
        String moduleTag = planContext.getTag();
        SerializableByteBuffer moduleBuffer = planContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = FesqlUtil.getMergedNodeOutputColumnList(node);
        RowTypeInfo finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);

        // TODO: Optimize not to encode and decode for each row
        DataStream<Row> outputDatastream = inputDatastream.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row inputRow) throws Exception {
                ByteBuffer moduleByteBuffer = moduleBuffer.getBuffer();
                JITManager.initJITModule(moduleTag, moduleByteBuffer);
                FeSQLJITWrapper jit = JITManager.getJIT(moduleTag);
                long functionPointer = jit.FindFunction(functionName);

                FesqlFlinkCodec inputCodec = new FesqlFlinkCodec(inputSchemaLists);
                FesqlFlinkCodec outputCodec = new FesqlFlinkCodec(outputSchemaLists);

                com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(inputRow);
                com._4paradigm.fesql.codec.Row outputNativeRow = CoreAPI.RowProject(functionPointer, inputFesqlRow, false);
                Row flinkRow = outputCodec.decodeFesqlRow(outputNativeRow);

                return flinkRow;
            }
        }).returns(finalOutputTypeInfo);

        // Convert DataStream<Row> to Table
        return planContext.getStreamTableEnvironment().fromDataStream(outputDatastream);

    }

}
