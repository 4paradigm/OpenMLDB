package com._4paradigm.fesql.stream;

import com._4paradigm.fesql.common.FesqlPlanContext;
import com._4paradigm.fesql.vm.PhysicalDataProviderNode;
import org.apache.flink.table.api.Table;

public class StreamDataProviderPlan {

    public static Table gen(FesqlPlanContext planContext, PhysicalDataProviderNode dataProviderNode) {
        String tableName = dataProviderNode.GetName();
        String sqlText = "select * from " + tableName;
        return planContext.getStreamTableEnvironment().sqlQuery(sqlText);
    }

}
