package com._4paradigm.fesql.batch;

import com._4paradigm.fesql.FeSqlLibrary;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.Engine;
import com._4paradigm.fesql.vm.PhysicalOpNode;
import com._4paradigm.fesql.vm.PhysicalOpType;
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

    private BatchTableEnvironment env;

    private Map<String, TableSchema> tableSchemaMap;

    public FesqlBatchPlanner(FesqlBatchTableEnvironment env) {
        this.env = env.getBatchTableEnvironment();
        this.tableSchemaMap = env.getRegisteredTableSchemaMap();
    }

    public Table plan(String sqlQuery) throws FeSQLException {

        TypeOuterClass.Database fesqlDatabase = FesqlUtil.buildDatabase("flink_db", this.tableSchemaMap);
        SQLEngine engine = new SQLEngine(sqlQuery, fesqlDatabase);

        PhysicalOpNode rootNode = engine.getPlan();
        rootNode.Print();

        Table table = visitPhysicalNode(rootNode);

        try {
            engine.close();
        } catch (Exception e) {
            throw new FeSQLException(String.format("Fail to close engine, error message: %s", e.getMessage()));
        }

        return table;

    }

    public Table visitPhysicalNode(PhysicalOpNode node) throws FeSQLException {

        List<Table> children = new ArrayList<Table>();
        for (int i=0; i < node.GetProducerCnt(); ++i) {
            children.add(visitPhysicalNode(node.GetProducer(i)));
        }

        PhysicalOpType opType = node.getType_();
        /*
        TODO: Add node planer to run Flink job
        if (opType instanceof PhysicalOpType.kPhysicalOpDataProvider) {
            //RowProjectPlan.gen(ctx, PhysicalTableProjectNode.CastFrom(projectNode), children)
        } else {
            throw new FeSQLException(String.format("Flink does not support physical op %s", node));
        }
         */

        return null;
    }

}
