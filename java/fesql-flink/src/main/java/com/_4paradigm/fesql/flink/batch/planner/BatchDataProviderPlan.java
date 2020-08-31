package com._4paradigm.fesql.flink.batch.planner;

import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.vm.PhysicalDataProviderNode;
import org.apache.flink.table.api.Table;

public class BatchDataProviderPlan {

    public static Table gen(GeneralPlanContext planContext, PhysicalDataProviderNode dataProviderNode) {
        return planContext.getBatchTableEnvironment().scan(dataProviderNode.GetName());
    }

}
