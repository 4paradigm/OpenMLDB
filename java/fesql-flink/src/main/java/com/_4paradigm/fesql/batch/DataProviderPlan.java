package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.vm.PhysicalDataProviderNode;
import org.apache.flink.table.api.Table;

public class DataProviderPlan {

    public static Table gen(PlanContext planContext, PhysicalDataProviderNode dataProviderNode) {
        String tableName = dataProviderNode.GetName();
        String sqlText = "select * from " + tableName;
        return planContext.getBatchTableEnvironment().sqlQuery(sqlText);
    }

}
