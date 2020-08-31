package com._4paradigm.fesql.flink.stream.planner;

import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.vm.PhysicalDataProviderNode;
import org.apache.flink.table.api.Table;

public class StreamDataProviderPlan {

    public static Table gen(GeneralPlanContext planContext, PhysicalDataProviderNode dataProviderNode) {
        return planContext.getStreamTableEnvironment().scan(dataProviderNode.GetName());
    }

}
