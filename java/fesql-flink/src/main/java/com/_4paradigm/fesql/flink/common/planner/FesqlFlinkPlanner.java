/*
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

package com._4paradigm.fesql.flink.common.planner;

import com._4paradigm.fesql.FeSqlLibrary;
import com._4paradigm.fesql.common.SQLEngine;
import com._4paradigm.fesql.common.UnsupportedFesqlException;
import com._4paradigm.fesql.flink.batch.FesqlBatchTableEnvironment;
import com._4paradigm.fesql.flink.batch.planner.*;
import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.flink.common.FesqlUtil;
import com._4paradigm.fesql.flink.stream.FesqlStreamTableEnvironment;
import com._4paradigm.fesql.flink.stream.planner.StreamDataProviderPlan;
import com._4paradigm.fesql.flink.stream.planner.StreamLimitPlan;
import com._4paradigm.fesql.flink.stream.planner.StreamTableProjectPlan;
import com._4paradigm.fesql.flink.stream.planner.StreamWindowAggPlan;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FesqlFlinkPlanner {

    private static final Logger logger = LoggerFactory.getLogger(FesqlFlinkPlanner.class);

    {
        // Ensure native initialized
        FeSqlLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    private boolean isBatch;
    private BatchTableEnvironment batchTableEnvironment;
    private StreamTableEnvironment streamTableEnvironment;

    private Map<String, TableSchema> tableSchemaMap;

    public FesqlFlinkPlanner(FesqlBatchTableEnvironment env) {
        this.isBatch = true;
        this.batchTableEnvironment = env.getBatchTableEnvironment();
        this.tableSchemaMap = env.getRegisteredTableSchemaMap();
    }

    public FesqlFlinkPlanner(FesqlStreamTableEnvironment env) {
        this.isBatch = false;
        this.streamTableEnvironment = env.getStreamTableEnvironment();
        this.tableSchemaMap = env.getRegisteredTableSchemaMap();
    }

    public Table plan(String sqlQuery) throws FesqlException, UnsupportedFesqlException {

        TypeOuterClass.Database fesqlDatabase = FesqlUtil.buildDatabase("flink_db", this.tableSchemaMap);
        SQLEngine engine = new SQLEngine(sqlQuery, fesqlDatabase);

        GeneralPlanContext planContext = null;
        if (this.isBatch) {
            planContext = new GeneralPlanContext(sqlQuery, this.batchTableEnvironment, this, engine.getIRBuffer());
        } else {
            planContext = new GeneralPlanContext(sqlQuery, this.streamTableEnvironment, this, engine.getIRBuffer());
        }

        PhysicalOpNode rootNode = engine.getPlan();
        logger.info("Print the FESQL logical plan");
        rootNode.Print();

        Table table = visitPhysicalNode(planContext, rootNode);

        try {
            engine.close();
        } catch (Exception e) {
            throw new FesqlException(String.format("Fail to close engine, error message: %s", e.getMessage()));
        }

        return table;

    }

    public Table visitPhysicalNode(GeneralPlanContext planContext, PhysicalOpNode node) throws FesqlException, UnsupportedFesqlException {

        List<Table> children = new ArrayList<Table>();
        for (int i=0; i < node.GetProducerCnt(); ++i) {
            children.add(visitPhysicalNode(planContext, node.GetProducer(i)));
        }

        Table outputTable = null;
        PhysicalOpType opType = node.GetOpType();

        if (opType.swigValue() == PhysicalOpType.kPhysicalOpDataProvider.swigValue()) { // DataProviderNode
            // Use "select *" to get Table from Flink source
            PhysicalDataProviderNode dataProviderNode = PhysicalDataProviderNode.CastFrom(node);

            if (isBatch) {
                outputTable = BatchDataProviderPlan.gen(planContext, dataProviderNode);
            } else {
                outputTable = StreamDataProviderPlan.gen(planContext, dataProviderNode);
            }

        } else if (opType.swigValue() == PhysicalOpType.kPhysicalOpSimpleProject.swigValue()) { // SimpleProjectNode
            PhysicalSimpleProjectNode physicalSimpleProjectNode = PhysicalSimpleProjectNode.CastFrom(node);
            // Batch and Streaming has the sample implementation
            outputTable = GeneralSimpleProjectPlan.gen(planContext, physicalSimpleProjectNode, children.get(0));

        } else if (opType.swigValue() == PhysicalOpType.kPhysicalOpProject.swigValue()) {
            // Use FESQL CoreAPI to generate new Table
            PhysicalProjectNode projectNode = PhysicalProjectNode.CastFrom(node);
            ProjectType projectType = projectNode.getProject_type_();

            if (projectType.swigValue() == ProjectType.kTableProject.swigValue()) { // TableProjectNode
                PhysicalTableProjectNode physicalTableProjectNode = PhysicalTableProjectNode.CastFrom(projectNode);

                if (isBatch) {
                    outputTable = BatchTableProjectPlan.gen(planContext, physicalTableProjectNode, children.get(0));
                } else {
                    outputTable = StreamTableProjectPlan.gen(planContext, physicalTableProjectNode, children.get(0));
                }

            } else if (projectType.swigValue() == ProjectType.kWindowAggregation.swigValue()) { // WindowAggNode
                PhysicalWindowAggrerationNode physicalWindowAggrerationNode = PhysicalWindowAggrerationNode.CastFrom(projectNode);

                if (isBatch) {
                    outputTable = BatchWindowAggPlan.gen(planContext, physicalWindowAggrerationNode, children.get(0));
                } else {
                    outputTable = StreamWindowAggPlan.gen(planContext, physicalWindowAggrerationNode, children.get(0));
                }

            } else if (projectType.swigValue() == ProjectType.kGroupAggregation.swigValue()) { // GroupbyAggNode
                PhysicalGroupAggrerationNode physicalGroupAggrerationNode = PhysicalGroupAggrerationNode.CastFrom(projectNode);
                if (isBatch) {
                    outputTable = BatchGroupbyAggPlan.gen(planContext, physicalGroupAggrerationNode, children.get(0));
                } else {
                    // TODO: need to convert upsert stream to Table, refer to https://github.com/apache/flink/pull/6787
                    throw new UnsupportedFesqlException(String.format("Planner does not support project type %s", projectType));
                }
            } else {
                throw new UnsupportedFesqlException(String.format("Planner does not support project type %s", projectType));
            }
        } else if (opType.swigValue() == PhysicalOpType.kPhysicalOpGroupBy.swigValue()) {
            PhysicalGroupNode physicalGroupNode = PhysicalGroupNode.CastFrom(node);
            outputTable = MockGroupbyPlan.gen(planContext, physicalGroupNode, children.get(0));
        } else if (opType.swigValue() == PhysicalOpType.kPhysicalOpLimit.swigValue()) {
            PhysicalLimitNode physicalLimitNode = PhysicalLimitNode.CastFrom(node);
            if (isBatch) {
                outputTable = BatchLimitPlan.gen(planContext, physicalLimitNode, children.get(0));
            } else {
                outputTable = StreamLimitPlan.gen(planContext, physicalLimitNode, children.get(0));
            }
        } else {
            throw new UnsupportedFesqlException(String.format("Planner does not support physical op %s", node));
        }

        return outputTable;
    }

}
