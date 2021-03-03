/*
 * java/fesql-flink/src/main/java/com/_4paradigm/fesql/flink/batch/planner/BatchTableProjectPlan.java
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
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.CoreAPI;
import com._4paradigm.fesql.vm.FeSQLJITWrapper;
import com._4paradigm.fesql.vm.PhysicalTableProjectNode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.List;

public class BatchTableProjectPlan {

    private static final Logger logger = LoggerFactory.getLogger(BatchTableProjectPlan.class);

    public static Table gen(GeneralPlanContext planContext, PhysicalTableProjectNode node, Table childTable) throws FesqlException {

        DataSet<Row> inputDataset = planContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);

        // Take out the serializable objects
        String functionName = node.project().fn_info().fn_name();
        String moduleTag = planContext.getTag();
        SerializableByteBuffer moduleBuffer = planContext.getModuleBuffer();

        List<List<TypeOuterClass.ColumnDef>> inputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node.GetProducer(0));
        List<List<TypeOuterClass.ColumnDef>> outputSchemaLists = FesqlUtil.getNodeOutputColumnLists(node);
        List<TypeOuterClass.ColumnDef> finalOutputSchema = node.GetOutputSchema();
        RowTypeInfo finalOutputTypeInfo = FesqlUtil.generateRowTypeInfo(finalOutputSchema);

        DataSet<Row> outputDataset = inputDataset.map(new RichMapFunction<Row, Row>() {

            long functionPointer;
            FesqlFlinkCodec inputCodec;
            FesqlFlinkCodec outputCodec;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ByteBuffer moduleByteBuffer = moduleBuffer.getBuffer();
                JITManager.initJITModule(moduleTag, moduleByteBuffer);
                FeSQLJITWrapper jit = JITManager.getJIT(moduleTag);
                functionPointer = jit.FindFunction(functionName);
                inputCodec = new FesqlFlinkCodec(inputSchemaLists);
                outputCodec = new FesqlFlinkCodec(outputSchemaLists);
            }

            @Override
            public Row map(Row inputRow) throws Exception {
                com._4paradigm.fesql.codec.Row inputFesqlRow = inputCodec.encodeFlinkRow(inputRow);
                com._4paradigm.fesql.codec.Row outputNativeRow = CoreAPI.RowProject(functionPointer, inputFesqlRow, false);
                return outputCodec.decodeFesqlRow(outputNativeRow);
            }

            @Override
            public void close() throws Exception {
                super.close();
                inputCodec.delete();
                outputCodec.delete();
            }

        }).returns(finalOutputTypeInfo);

        // Convert DataSet<Row> to Table
        return planContext.getBatchTableEnvironment().fromDataSet(outputDataset);
    }

}
