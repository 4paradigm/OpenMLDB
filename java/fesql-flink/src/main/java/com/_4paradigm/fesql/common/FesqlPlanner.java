package com._4paradigm.fesql.common;

import com._4paradigm.fesql.FeSqlLibrary;
import com._4paradigm.fesql.batch.DataProviderPlan;
import com._4paradigm.fesql.batch.FesqlBatchTableEnvironment;
import com._4paradigm.fesql.batch.TableProjectPlan;
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

public class FesqlPlanner {

    private static final Logger logger = LoggerFactory.getLogger(FesqlPlanner.class);

    {
        // Ensure native initialized
        FeSqlLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    private BatchTableEnvironment batchTableEnvironment;

    private Map<String, TableSchema> tableSchemaMap;

    public FesqlPlanner(FesqlBatchTableEnvironment env) {
        this.batchTableEnvironment = env.getBatchTableEnvironment();
        this.tableSchemaMap = env.getRegisteredTableSchemaMap();
    }

    public Table plan(String sqlQuery) throws FesqlException {

        TypeOuterClass.Database fesqlDatabase = FesqlUtil.buildDatabase("flink_db", this.tableSchemaMap);
        SQLEngine engine = new SQLEngine(sqlQuery, fesqlDatabase);
        FesqlPlanContext batchPlanContext = new FesqlPlanContext(sqlQuery, batchTableEnvironment, this, engine.getIRBuffer());

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

    public Table visitPhysicalNode(FesqlPlanContext batchPlanContext, PhysicalOpNode node) throws FesqlException {

        List<Table> children = new ArrayList<Table>();
        for (int i=0; i < node.GetProducerCnt(); ++i) {
            children.add(visitPhysicalNode(batchPlanContext, node.GetProducer(i)));
        }

        Table outputTable = null;
        PhysicalOpType opType = node.getType_();

        /* TODO: support simple project node
            import static org.apache.flink.table.api.Expressions.$;
            table.select($("vendor_sum_pl"), $("vendor_sum_pl"));
        */

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
