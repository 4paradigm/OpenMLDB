package com._4paradigm.fesql.batch.planner;

import com._4paradigm.fesql.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.vm.PhysicalDataProviderNode;
import org.apache.flink.table.api.Table;

public class BatchDataProviderPlan {

    public static Table gen(GeneralPlanContext planContext, PhysicalDataProviderNode dataProviderNode) {
        String tableName = dataProviderNode.GetName();
        String sqlText = "select * from " + tableName;
        return planContext.getBatchTableEnvironment().sqlQuery(sqlText);
    }

}
