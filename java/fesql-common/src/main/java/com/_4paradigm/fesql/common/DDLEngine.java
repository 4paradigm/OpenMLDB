package com._4paradigm.fesql.common;
import com._4paradigm.fesql.FeSqlLibrary;

import com._4paradigm.fesql.node.ColumnRefNode;
import com._4paradigm.fesql.node.ExprListNode;
import com._4paradigm.fesql.node.ExprNode;
import com._4paradigm.fesql.node.ExprType;
import com._4paradigm.fesql.tablet.Tablet;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.Data;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DDLEngine {
    static {
        // Ensure native initialized
        FeSqlLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    public static int resolveColumnIndex(ExprNode expr, PhysicalOpNode planNode) throws FesqlException {
        if (expr.getExpr_type_() == ExprType.kExprColumnRef) {
            int index = CoreAPI.ResolveColumnIndex(planNode, ColumnRefNode.CastFrom(expr));
            if (index >= 0 && index <= planNode.GetOutputSchema().size()) {
                return index;
            } else {
                throw new FesqlException("Fail to resolve {} with index = {}".format(expr.GetExprString(), index));
            }
        }
        throw new FesqlException("Expr {} not supported".format(expr.GetExprString()));
    }

    /**
     *
     * @param sql
     * @param schema json format
     * @return
     */
    public static String genDDL(String sql, String schema) {
        String tempDB = "temp_" + System.currentTimeMillis();
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName(tempDB);
        List<TypeOuterClass.TableDef> tables = getTableDefs(schema);
        Map<String, TypeOuterClass.TableDef> tableDefMap = new HashMap<>();
        for (TypeOuterClass.TableDef e : tables) {
            db.addTables(e);
            tableDefMap.put(e.getName(), e);
        }
        try {
            RequestEngine engine = new RequestEngine(sql, db.build());
            // SQLEngine engine = new SQLEngine(sql, db.build());
            PhysicalOpNode plan = engine.getPlan();
            List<PhysicalOpNode> listNodes = new ArrayList<PhysicalOpNode>();
            dagToList(plan, listNodes);
            parseRtidbIndex(listNodes, tableDefMap);
            plan.Print();
            System.out.println("plan info");
            System.out.println(plan.GetProducerCnt());
        } catch (UnsupportedFesqlException e) {
            e.printStackTrace();
        } catch (FesqlException e) {
            e.printStackTrace();
        }
        return "xx";
    }

    public static List<RtidbTable> parseRtidbIndex(List<PhysicalOpNode> nodes, Map<String, TypeOuterClass.TableDef> tableDefMap) throws FesqlException {
        List<RtidbTable> rtidbTables = new ArrayList<>();
        Map<String, String> table2OrgTable = new HashMap<>();
        for (PhysicalOpNode node : nodes) {
            System.out.println("node type = " + node.GetTypeName());
            PhysicalOpType type = node.getType_();
            if (type.swigValue() == PhysicalOpType.kPhysicalOpDataProvider.swigValue()) {
                PhysicalDataProviderNode castNode = PhysicalDataProviderNode.CastFrom(node);
                System.out.println("PhysicalDataProviderNode = " + castNode.GetName());
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpSimpleProject.swigValue()) {
                PhysicalSimpleProjectNode castNode = PhysicalSimpleProjectNode.CastFrom(node);
                System.out.println("PhysicalSimpleProjectNode ");
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpConstProject.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRequestUnoin.swigValue()) {
                System.out.println("kPhysicalOpRequestUnoin ");
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpProject.swigValue()) {
                PhysicalProjectNode projectNode = PhysicalProjectNode.CastFrom(node);
                if (projectNode.getProject_type_().swigValue() == ProjectType.kRowProject.swigValue()) {
                    continue;
                }
                if (projectNode.getProject_type_().swigValue() == ProjectType.kWindowAggregation.swigValue()) {
                    System.out.println("kWindowAggregation ");
                    PhysicalWindowAggrerationNode castNode = PhysicalWindowAggrerationNode.CastFrom(projectNode);
                    WindowUnionList wuList = castNode.window_unions();
                    for (int i = 0; i < wuList.GetSize(); i++) {
                        PhysicalOpNode unionNode = castNode.window_unions().GetUnionNode(i);
                        System.out.println("window union = " + unionNode.GetTypeName());
                    }
                    continue;
                }
                if (projectNode.getProject_type_().swigValue() == ProjectType.kTableProject.swigValue()) {
                    continue;
                }

                // PhysicalWindowAggrerationNode.CastFrom(node)
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpGroupBy.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRequestJoin.swigValue()) {
                PhysicalRequestJoinNode join = PhysicalRequestJoinNode.CastFrom(node);
                System.out.println(join.GetProducer(0).GetTypeName());
                System.out.println(join.GetProducer(1).GetTypeName());
                System.out.println("leftkey type = " + join.join().left_key().getKeys_().GetChild(0).GetTypeName());
                System.out.println("rightkey type = " + join.join().right_key().getKeys_().GetChild(0).GetTypeName());
                ColumnRefNode columnNode = ColumnRefNode.CastFrom(join.getJoin_().left_key().getKeys_().GetChild(0));
                System.out.println("leftkey name = " + columnNode.GetColumnName());
                System.out.println("leftkey name = " + columnNode.GetRelationName());
                columnNode = ColumnRefNode.CastFrom(join.getJoin_().right_key().getKeys_().GetChild(0));
                System.out.println("rightKey name = " + columnNode.GetColumnName());
                System.out.println("rightKey name = " + columnNode.GetRelationName());
//                System.out.println(join.getJoin_().FnDetail());
                PhysicalDataProviderNode dataNode = findDataProviderNode(join.GetProducer(0));
                if (dataNode != null) {
                    // PhysicalDataProviderNode dataNode = PhysicalDataProviderNode.CastFrom(join.GetProducer(0));
                    System.out.println(dataNode.GetName());
                }
                dataNode = findDataProviderNode(join.GetProducer(1));
                if (dataNode != null) {
                    // PhysicalDataProviderNode dataNode = PhysicalDataProviderNode.CastFrom(join.GetProducer(1));
                    System.out.println(dataNode.GetName());
                }

                ExprListNode rightNode = join.getJoin_().right_key().getKeys_();

                for (int i = 0; i < rightNode.GetChildNum(); i++) {
                    // System.out.println(join.GetProducer(1));
                    int index = resolveColumnIndex(rightNode.GetChild(i), join.GetProducer(1));
                    System.out.println(index);

                }

                ExprListNode leftNode = join.getJoin_().left_key().getKeys_();

                for (int i = 0; i < leftNode.GetChildNum(); i++) {
                    // System.out.println(join.GetProducer(0));
                    int index = resolveColumnIndex(leftNode.GetChild(i), join.GetProducer(0));
                    // System.out.println(index);
                    
                }
//                System.out.println(join.getJoin_().getRight_sort_().orders().GetExprString());
                // System.out.println(join.getJoin_().right_key().getKeys_().GetChild(0));
                // System.out.println(join.GetFnName());
                // System.out.println(join.getJoin_().left_key().getKeys_().getChildren_());
                // System.out.println(node.GetFnInfo().getFn_name_());
                // System.out.println("over");
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpLimit.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRename.swigValue()) {
                PhysicalRenameNode castNode = PhysicalRenameNode.CastFrom(node);
                System.out.println("rename = " + castNode.getName_());
                PhysicalDataProviderNode dataNode = findDataProviderNode(node.GetProducer(0));
                if (dataNode != null) {
                    table2OrgTable.put(castNode.getName_(), dataNode.GetName());
                }
                continue;
            }
        }
        return rtidbTables;
    }
    
    public static PhysicalDataProviderNode findDataProviderNode(PhysicalOpNode node) {
        if (node.getType_() == PhysicalOpType.kPhysicalOpDataProvider) {
            return PhysicalDataProviderNode.CastFrom(node);
        }
        if (node.getType_() == PhysicalOpType.kPhysicalOpSimpleProject) {
            return findDataProviderNode(node.GetProducer(0));
        }
        if (node.getType_() == PhysicalOpType.kPhysicalOpRename) {
            return findDataProviderNode(node.GetProducer(0));
        }
        return null;

    }

    public static void dagToList(PhysicalOpNode node, List<PhysicalOpNode> list) {
//        com._4paradigm.fesql.vm.PhysicalOpType type = node.getType_();
        for (long i = 0; i < node.GetProducerCnt(); i++) {
            dagToList(node.GetProducer(i), list);
        }
        list.add(node);



    }

    public static TypeOuterClass.Type getFesqlType(String type) {
        if (type.equalsIgnoreCase("bigint") || type.equalsIgnoreCase("long")) {
            return TypeOuterClass.Type.kInt64;
        }
        if (type.equalsIgnoreCase("smallint") || type.equalsIgnoreCase("small")) {
            return TypeOuterClass.Type.kInt16;
        }
        if (type.equalsIgnoreCase("int")) {
            return TypeOuterClass.Type.kInt32;
        }
        if (type.equalsIgnoreCase("float")) {
            return TypeOuterClass.Type.kFloat;
        }
        if (type.equalsIgnoreCase("double")) {
            return TypeOuterClass.Type.kDouble;
        }
        if (type.equalsIgnoreCase("string")) {
            return TypeOuterClass.Type.kVarchar;
        }
        if (type.equalsIgnoreCase("boolean") || type.equalsIgnoreCase("bool")) {
            return TypeOuterClass.Type.kBool;
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return TypeOuterClass.Type.kTimestamp;
        }
        if (type.equalsIgnoreCase("date")) {
            return TypeOuterClass.Type.kDate;
        }
        return null;
    }

    public static List<TypeOuterClass.TableDef> getTableDefs(String jsonObject) {
        List<TypeOuterClass.TableDef> tableDefs = new ArrayList<>();
//        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();
        JsonElement tableJson = jsonParser.parse(jsonObject);
        for (Map.Entry<String, JsonElement> e : tableJson.getAsJsonObject().get("tableInfo").getAsJsonObject().entrySet()) {
            TypeOuterClass.TableDef.Builder table = TypeOuterClass.TableDef.newBuilder();
            table.setName(e.getKey());
            for (JsonElement element : e.getValue().getAsJsonArray()) {
                table.addColumns(TypeOuterClass.ColumnDef.newBuilder()
                        .setName(element.getAsJsonObject().get("name").getAsString())
                        .setIsNotNull(false)
                        .setType(getFesqlType(element.getAsJsonObject().get("type").getAsString())));
            }
            tableDefs.add(table.build());
        }
        return tableDefs;
    }


    public static void main(String[] args) {
//        String schemaPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/homecredit.json";
//        String sqlPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/homecredit.txt";
        String schemaPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/rong_e.json";
        String sqlPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/rong_e.txt";
        File file = new File(schemaPath);
        File sql = new File(sqlPath);
        try {
            genDDL(FileUtils.readFileToString(sql, "UTF-8"), FileUtils.readFileToString(file, "UTF-8"));
//            getTableDefs(FileUtils.readFileToString(file, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


class RtidbTable {
    TypeOuterClass.TableDef schema;
    Set<RtidbIndex> indexs = new HashSet<RtidbIndex>();
}

enum TTLType {
    kAbsolute,
    kLatest,
    kAbsOrLat,
    kAbsAndLat
}

@Data
class RtidbIndex {
    private List<String> keys = new ArrayList<>();
    private String ts = "";
    private TTLType type = TTLType.kAbsOrLat;
    // 映射到ritdb是最多保留多少条数据，不是最少
    private long atmost = 0;
    private long expire = 0;
//    @Override
    public boolean equals(RtidbIndex e) {
        return  this.getType() == e.getType() && this.getKeys().equals(e.getKeys()) && this.ts.equals(e.getTs());
    }

}
