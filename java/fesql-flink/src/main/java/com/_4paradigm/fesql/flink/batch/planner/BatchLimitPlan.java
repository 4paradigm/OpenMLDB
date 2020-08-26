package com._4paradigm.fesql.flink.batch.planner;

import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.vm.PhysicalLimitNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BatchLimitPlan {

    private static final Logger logger = LoggerFactory.getLogger(BatchLimitPlan.class);

    private static int currentCnt = 0;

    public static Table gen(GeneralPlanContext planContext, PhysicalLimitNode node, Table childTable) throws FesqlException {

        DataSet<Row> inputDataset = planContext.getBatchTableEnvironment().toDataSet(childTable, Row.class);
        final int limitCnt = node.GetLimitCnt();

        DataSet<Row> outputDataset = inputDataset.filter(new FilterFunction<Row>() {
            @Override
            public boolean filter(Row row) throws Exception {
                if (currentCnt < limitCnt) {
                    currentCnt++;
                    return true;
                } else {
                    return false;
                }
            }
        });

        // Convert DataSet<Row> to Table
        return planContext.getBatchTableEnvironment().fromDataSet(outputDataset);
    }

}
