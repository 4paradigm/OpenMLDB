package com._4paradigm.fesql.flink.stream.planner;

import com._4paradigm.fesql.flink.common.*;
import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.vm.PhysicalLimitNode;
import com._4paradigm.fesql.vm.PhysicalTableProjectNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamLimitPlan {

    private static final Logger logger = LoggerFactory.getLogger(StreamLimitPlan.class);
    private static int currentCnt = 0;

    public static Table gen(GeneralPlanContext planContext, PhysicalLimitNode node, Table childTable) throws FesqlException {

        DataStream<Row> inputDatastream = planContext.getStreamTableEnvironment().toAppendStream(childTable, Row.class);
        final int limitCnt = node.GetLimitCnt();

        DataStream<Row> outputDatastream = inputDatastream.filter(new FilterFunction<Row>() {
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

        // Convert DataStream<Row> to Table
        return planContext.getStreamTableEnvironment().fromDataStream(outputDatastream);
    }

}
