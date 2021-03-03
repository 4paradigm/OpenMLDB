/*
 * java/fesql-flink/src/main/java/com/_4paradigm/fesql/flink/stream/planner/StreamLimitPlan.java
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

package com._4paradigm.fesql.flink.stream.planner;

import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.flink.common.planner.GeneralPlanContext;
import com._4paradigm.fesql.vm.PhysicalLimitNode;
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
