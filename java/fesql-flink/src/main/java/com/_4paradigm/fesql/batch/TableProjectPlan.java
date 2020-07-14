package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.vm.PhysicalTableProjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class TableProjectPlan {

    public static Table gen(PlanContext planContext, PhysicalTableProjectNode node, Table childTable) {

        DataSet<Row> inputDataset = planContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);

        DataSet<Row> outputDataset = inputDataset.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                return row;
            }
        });//.returns(new RowTypeInfo(Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE));;

        return planContext.getBatchTableEnvironment().fromDataSet(outputDataset);

    }

}
