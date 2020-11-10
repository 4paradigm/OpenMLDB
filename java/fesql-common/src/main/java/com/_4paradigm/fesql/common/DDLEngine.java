package com._4paradigm.fesql.common;
import com._4paradigm.fesql.FeSqlLibrary;

import com._4paradigm.fesql.tablet.Tablet;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.vm.Engine;
import com._4paradigm.fesql.vm.PhysicalJoinNode;
import com._4paradigm.fesql.vm.PhysicalOpNode;
import com._4paradigm.fesql.vm.PhysicalOpType;
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
            SQLEngine engine = new SQLEngine(sql, db.build());
            PhysicalOpNode plan = engine.getPlan();
            List<PhysicalOpNode> listNodes = new ArrayList<PhysicalOpNode>();
            dagToList(plan, listNodes);
            parseRtidbIndex(listNodes, tableDefMap);
            plan.Print();
            System.out.println("plan info");
            System.out.println(plan.GetProducerCnt());
        } catch (UnsupportedFesqlException e) {
            e.printStackTrace();
        }
        return "xx";
    }

    public static List<RtidbTable> parseRtidbIndex(List<PhysicalOpNode> nodes, Map<String, TypeOuterClass.TableDef> tableDefMap) {
        List<RtidbTable> rtidbTables = new ArrayList<>();
        for (PhysicalOpNode node : nodes) {
            PhysicalOpType type = node.GetOpType();
            if (type.swigValue() == PhysicalOpType.kPhysicalOpDataProvider.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpSimpleProject.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpConstProject.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpProject.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpGroupBy.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpJoin.swigValue()) {
                PhysicalJoinNode join = PhysicalJoinNode.CastFrom(node);
                System.out.println(join.getJoin_().FnDetail());
//                System.out.println(join.getJoin_().getRight_sort_().orders().GetExprString());
                System.out.println(join.getJoin_().right_key().keys().GetExprString());
                System.out.println(join.getJoin_().left_key().keys().GetExprString());
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpLimit.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRename.swigValue()) {
                continue;
            }
        }
        return rtidbTables;
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
        String schemaPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/homecredit.json";
        String sqlPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/homecredit.txt";
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
