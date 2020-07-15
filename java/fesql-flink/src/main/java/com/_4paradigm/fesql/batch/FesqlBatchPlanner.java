package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.FeSqlLibrary;
import com._4paradigm.fesql.common.FesqlException;
import com._4paradigm.fesql.common.FesqlUtil;
import com._4paradigm.fesql.common.SQLEngine;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FesqlBatchPlanner {

    private Logger logger = LoggerFactory.getLogger(FesqlBatchPlanner.class);

    {
        // Ensure native initialized
        FeSqlLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    private BatchTableEnvironment batchTableEnvironment;

    private Map<String, TableSchema> tableSchemaMap;

    public FesqlBatchPlanner(FesqlBatchTableEnvironment env) {
        this.batchTableEnvironment = env.getBatchTableEnvironment();
        this.tableSchemaMap = env.getRegisteredTableSchemaMap();
    }

    public Table plan(String sqlQuery) throws FesqlException {

        TypeOuterClass.Database fesqlDatabase = FesqlUtil.buildDatabase("flink_db", this.tableSchemaMap);
        SQLEngine engine = new SQLEngine(sqlQuery, fesqlDatabase);
        BatchPlanContext batchPlanContext = new BatchPlanContext(sqlQuery, batchTableEnvironment, this, engine.getIRBuffer());

        PhysicalOpNode rootNode = engine.getPlan();
        logger.info("Print the FESQL logical plan");
        rootNode.Print();

        Table table = visitPhysicalNode(batchPlanContext, rootNode);

        try {
            engine.close();
        } catch (Exception e) {
            throw new FesqlException(String.format("Fail to close engine, error message: %s", e.getMessage()));
        }

        return table;

    }

    public Table visitPhysicalNode(BatchPlanContext batchPlanContext, PhysicalOpNode node) throws FesqlException {

        List<Table> children = new ArrayList<Table>();
        for (int i=0; i < node.GetProducerCnt(); ++i) {
            children.add(visitPhysicalNode(batchPlanContext, node.GetProducer(i)));
        }

        Table outputTable = null;
        PhysicalOpType opType = node.getType_();

        if (opType.swigValue() == PhysicalOpType.kPhysicalOpDataProvider.swigValue()) {
            // Use "select *" to get Table from Flink source
            PhysicalDataProviderNode dataProviderNode = PhysicalDataProviderNode.CastFrom(node);
            outputTable = DataProviderPlan.gen(batchPlanContext, dataProviderNode);

        } else if (opType.swigValue() == PhysicalOpType.kPhysicalOpProject.swigValue()) {
            // Use FESQL CoreAPI to generate new Table
            PhysicalProjectNode projectNode = PhysicalProjectNode.CastFrom(node);
            ProjectType projectType = projectNode.getProject_type_();

            if (projectType.swigValue() == ProjectType.kTableProject.swigValue()) {
                PhysicalTableProjectNode physicalTableProjectNode = PhysicalTableProjectNode.CastFrom(projectNode);
                outputTable = TableProjectPlan.gen(batchPlanContext, physicalTableProjectNode, children.get(0));
            } else {
                throw new FesqlException(String.format("Planner does not support project type %s", projectType));
            }
        } else {
            throw new FesqlException(String.format("Planner does not support physical op %s", node));
        }

        return outputTable;
    }

}
