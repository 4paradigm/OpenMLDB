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

package com._4paradigm.openmldb.batch.utils;

import com._4paradigm.hybridse.HybridSeLibrary;
import com._4paradigm.hybridse.node.ColumnRefNode;
import com._4paradigm.hybridse.node.ExprNode;
import com._4paradigm.hybridse.node.ExprType;
import com._4paradigm.hybridse.sdk.HybridSeException;
import com._4paradigm.hybridse.sdk.RequestEngine;
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException;
import com._4paradigm.hybridse.type.TypeOuterClass;
import com._4paradigm.hybridse.vm.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;


public class DDLEngine {
    static {
        // Ensure native initialized
        HybridSeLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    private static final Logger logger = LoggerFactory.getLogger(DDLEngine.class);

    public static final String SQLTableName = "sql_table";

    public static int resolveColumnIndex(ExprNode expr, PhysicalOpNode planNode) throws HybridSeException {
        if (expr.getExpr_type_() == ExprType.kExprColumnRef) {
            int index = CoreAPI.ResolveColumnIndex(planNode, ColumnRefNode.CastFrom(expr));
            if (index >= 0 && index <= planNode.GetOutputSchema().size()) {
                return index;
            } else {
                throw new HybridSeException("Fail to resolve {} with index = {}".format(expr.GetExprString(), index));
            }
        }
        throw new HybridSeException("Expr {} not supported".format(expr.GetExprString()));
    }

    public static List<LocatedFileStatus> listFiles(Configuration conf, String path, String ends) {
        List<LocatedFileStatus> files = new ArrayList<>();
        try {
            FileSystem fs = FileSystem.get(new URI(path), conf);
            RemoteIterator<LocatedFileStatus> fileStatus = fs.listFiles(new Path(path), true);
            if (fileStatus != null) {
                while (fileStatus.hasNext()) {
                    LocatedFileStatus cur = fileStatus.next();
                    if (cur.isFile()) {
                        String fileName = cur.getPath().getName();
                        if (fileName.startsWith(".") || fileName.startsWith("_")) {
                            continue;
                        }
                        if (ends == null || fileName.endsWith(ends)) {
                            files.add(cur);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return files;
    }

    public static MessageType readParquetSchema(String path) {
        Configuration _default = new Configuration();
        List<LocatedFileStatus> files = listFiles(_default, path, null);
        if (files.isEmpty()) {
            throw new RuntimeException(String.format("Empty path [%s] ", path));
        }
        Path headPath = files.get(0).getPath();
        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(headPath, _default))) {
            return r.getFooter().getFileMetaData().getSchema();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Error read Parquet file schema, path [%s].", headPath), e);
        }
    }

    public static String genFedbDdl(Map<String, String> nameParquetMap, String sql, int replicaNum, int partitionNum) throws Exception {
        String tempDB = "temp_" + System.currentTimeMillis();
        String replicaAndPartition = String.format(" options ( replicanum=%s, partitionnum=%s ) ", replicaNum, partitionNum);
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName(tempDB);

        List<TypeOuterClass.TableDef> tableDefs = new ArrayList<>();
        Map<String, TypeOuterClass.TableDef> tableDefMap = new HashMap<>();

        // Iterate all registered tables
        for (Map.Entry<String, String> map: nameParquetMap.entrySet()) {
            String tableName = map.getKey();
            String parquetPath = map.getValue();
            MessageType schema = readParquetSchema(parquetPath);
            List<Type> fields = schema.getFields();
            TypeOuterClass.TableDef.Builder table = TypeOuterClass.TableDef.newBuilder();
            table.setName(tableName);

            for (Type field: fields) {
                table.addColumns(TypeOuterClass.ColumnDef.newBuilder()
                        .setName(field.getName())
                        .setIsNotNull(false)
                        .setType(parquetTypeToFedbType(field)));
            }

            TypeOuterClass.TableDef tableDef = table.build();

            tableDefs.add(tableDef);
            db.addTables(tableDef);
            tableDefMap.put(tableName, tableDef);
        }

        try {
            RequestEngine engine = new RequestEngine(sql, db.build());
            PhysicalOpNode plan = engine.getPlan();
            List<PhysicalOpNode> listNodes = new ArrayList<>();
            dagToList(plan, listNodes);
            Map<String, RtidbTable> rtidbTables = parseRtidbIndex(listNodes, tableDefMap);
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, RtidbTable> e : rtidbTables.entrySet()) {
                if (e.getKey().equals(e.getValue().getTableName())) {
                    sb.append(e.getValue().toDDL());
                    sb.append(replicaAndPartition);
                    sb.append(";\n");
                }
            }
            String res = sb.toString();
            return res;
        } catch (UnsupportedHybridSeException | HybridSeException e) {
            e.printStackTrace();
            throw new Exception("failed to gen ddl for " + sql + ", exception: " + e.getMessage());
        }
    }

    public static String genDDL(String sql, String schema, int replicanum, int partitionnum) throws Exception {
        String tempDB = "temp_" + System.currentTimeMillis();
        String replicaAndPartition = String.format(" options ( replicanum=%s, partitionnum=%s ) ", replicanum, partitionnum);
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
            PhysicalOpNode plan = engine.getPlan();
            List<PhysicalOpNode> listNodes = new ArrayList<>();
            dagToList(plan, listNodes);
            Map<String, RtidbTable> rtidbTables = parseRtidbIndex(listNodes, tableDefMap);
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, RtidbTable> e : rtidbTables.entrySet()) {
                if (e.getKey().equals(e.getValue().getTableName())) {
                    sb.append(e.getValue().toDDL());
                    sb.append(replicaAndPartition);
                    sb.append(";\n");
                }
            }
            return sb.toString();
        } catch (UnsupportedHybridSeException | HybridSeException e) {
            e.printStackTrace();
        }
        throw new Exception("failed to gen ddl for " + schema);
    }

    /**
     *
     */
    public static String genDDL(String sql, String schema) throws Exception {
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
            PhysicalOpNode plan = engine.getPlan();
            List<PhysicalOpNode> listNodes = new ArrayList<>();
            dagToList(plan, listNodes);
            Map<String, RtidbTable> rtidbTables = parseRtidbIndex(listNodes, tableDefMap);
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, RtidbTable> e : rtidbTables.entrySet()) {
                if (e.getKey().equals(e.getValue().getTableName())) {
                    sb.append(e.getValue().toDDL());
                    sb.append(";\n");
                }
            }
            String res = sb.toString();
            return res;
        } catch (UnsupportedHybridSeException | HybridSeException e) {
            e.printStackTrace();
        }
        throw new Exception("failed to gen ddl");
    }

    /**
     * 只对window op做解析，因为fesql node类型太多了，暂时没办法做通用性解析
     */
    public static void parseWindowOp(PhysicalOpNode node, Map<String, RtidbTable> rtidbTables) {
        logger.info("begin to pares window op");
        PhysicalRequestUnionNode castNode = PhysicalRequestUnionNode.CastFrom(node);
        long start = 0;
        //long end = 0;
        long cntStart = 0;
        //long cntEnd = 0;
        if (castNode.window().range().frame().frame_range() != null) {
            start = Math.abs(Long.valueOf(castNode.window().range().frame().frame_range().start().GetExprString()));
            //end = Math.abs(Long.valueOf(castNode.window().range().frame().frame_range().end().GetExprString()));
        }
        if (castNode.window().range().frame().frame_rows() != null) {
            cntStart = Math.abs(Long.valueOf(castNode.window().range().frame().frame_rows().start().GetExprString()));
            //cntEnd = Math.abs(Long.valueOf(castNode.window().range().frame().frame_rows().end().GetExprString()));
        }
        List<PhysicalOpNode> nodes = new ArrayList<>();
        for (int i = 0; i < castNode.GetProducerCnt(); i++) {
            nodes.add(castNode.GetProducer(i));
        }
        for (int i = 0; i < castNode.window_unions().GetSize(); i++) {
            nodes.add(castNode.window_unions().GetKey(i));
        }

//        int nodeIndex = -1;
        for (PhysicalOpNode e : nodes) {
//            nodeIndex++;
            PhysicalDataProviderNode unionTable = findDataProviderNode(e);
            logger.info("union table = {}", unionTable.GetName());

            String table = unionTable.GetName();
            RtidbTable rtidbTable = rtidbTables.get(table);

            RtidbIndex index = new RtidbIndex();
            List<String> keys = index.getKeys();
            for (int keyIndex = 0; keyIndex < castNode.window().partition().keys().GetChildNum(); keyIndex++) {
                String key = CoreAPI.ResolveSourceColumnName(e,
                        ColumnRefNode.CastFrom(castNode.window().partition().keys().GetChild(keyIndex)));
                keys.add(key);
            }
            String ts = CoreAPI.ResolveSourceColumnName(e, ColumnRefNode.CastFrom(castNode.window().sort().orders().order_by().GetChild(0)));

            index.setTs(ts);
            if (start != 0) {
                if (start < 60 * 1000) {
                    // 60秒
                    index.setExpire(60 * 1000);
                } else {
                    index.setExpire(start);
                }
            }
            if (cntStart != 0) {
                index.setAtmost(cntStart);
            }
            if (index.getAtmost() > 0 && index.getExpire() == 0) {
                index.setType(TTLType.kLatest);
            }
            if (index.getAtmost() == 0 && index.getExpire() > 0) {
                index.setType(TTLType.kAbsolute);
            }
            rtidbTable.addIndex(index);
        }
        logger.info("end to pares window op");
    }

    public static void parseLastJoinOp(PhysicalOpNode node, Map<String, RtidbTable> rtidbTables) {
        logger.info("begin to pares lastjoin op");
        PhysicalRequestJoinNode join = PhysicalRequestJoinNode.CastFrom(node);
        PhysicalDataProviderNode dataNode = findDataProviderNode(join.GetProducer(0));
        //String leftName = "";
        String rightName = "";
        if (dataNode != null) {
            // PhysicalDataProviderNode dataNode = PhysicalDataProviderNode.CastFrom(join.GetProducer(0));
            logger.info(dataNode.GetName());
            //leftName = dataNode.GetName();
        }
        dataNode = findDataProviderNode(join.GetProducer(1));
        if (dataNode != null) {
            // PhysicalDataProviderNode dataNode = PhysicalDataProviderNode.CastFrom(join.GetProducer(0));
            logger.info(dataNode.GetName());
            rightName = dataNode.GetName();
        } else {
            return;
        }
        RtidbTable rightTable = rtidbTables.get(rightName);
        RtidbIndex rightIndex = new RtidbIndex();

        Key conditionKey = join.join().right_key();
        Sort sort = join.join().right_sort();
        if (sort != null && sort.orders() != null) {
            String ts = CoreAPI.ResolveSourceColumnName(join, ColumnRefNode.CastFrom(sort.orders().order_by().GetChild(0)));
            rightIndex.setTs(ts);
        }
        rightIndex.setAtmost(1);
        List<String> keys = rightIndex.getKeys();
        for (int i = 0; i < conditionKey.keys().GetChildNum(); i++) {
            String keyName = CoreAPI.ResolveSourceColumnName(node,
                    ColumnRefNode.CastFrom(conditionKey.keys().GetChild(i)));
            keys.add(keyName);
        }
        rightIndex.setType(TTLType.kLatest);
        rightTable.addIndex(rightIndex);
        logger.info("begin to pares lastjoin op");
    }

    public static Map<String, RtidbTable> parseRtidbIndex(List<PhysicalOpNode> nodes, Map<String, TypeOuterClass.TableDef> tableDefMap) throws HybridSeException {
        Map<String, RtidbTable> rtidbTables = new HashMap<>();
        Map<String, String> table2OrgTable = new HashMap<>();
        for (PhysicalOpNode node : nodes) {
            PhysicalOpType type = node.GetOpType();
            if (type.swigValue() == PhysicalOpType.kPhysicalOpDataProvider.swigValue()) {
                PhysicalDataProviderNode castNode = PhysicalDataProviderNode.CastFrom(node);
                RtidbTable rtidbTable = rtidbTables.get(castNode.GetName());
                if (rtidbTable == null) {
                    rtidbTable = new RtidbTable();
                    rtidbTable.setTableName(castNode.GetName());
                    rtidbTable.setSchema(tableDefMap.get(castNode.GetName()));
                    rtidbTables.put(castNode.GetName(), rtidbTable);
                }
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRequestUnion.swigValue()) {
                parseWindowOp(node, rtidbTables);
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRequestJoin.swigValue()) {
                parseLastJoinOp(node, rtidbTables);
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpLimit.swigValue()) {
                continue;
            }
            if (type.swigValue() == PhysicalOpType.kPhysicalOpRename.swigValue()) {
                PhysicalRenameNode castNode = PhysicalRenameNode.CastFrom(node);
                logger.info("rename = {}", castNode.getName_());
                PhysicalDataProviderNode dataNode = findDataProviderNode(node.GetProducer(0));
                if (dataNode != null) {
                    table2OrgTable.put(castNode.getName_(), dataNode.GetName());
                    rtidbTables.put(castNode.getName_(), rtidbTables.get(dataNode.GetName()));
                }
                continue;
            }
        }
        return rtidbTables;
    }

    public static PhysicalDataProviderNode findDataProviderNode(PhysicalOpNode node) {
        if (node.GetOpType() == PhysicalOpType.kPhysicalOpDataProvider) {
            return PhysicalDataProviderNode.CastFrom(node);
        }
        if (node.GetOpType() == PhysicalOpType.kPhysicalOpSimpleProject) {
            return findDataProviderNode(node.GetProducer(0));
        }
        if (node.GetOpType() == PhysicalOpType.kPhysicalOpRename) {
            return findDataProviderNode(node.GetProducer(0));
        }
        return null;

    }

    public static void dagToList(PhysicalOpNode node, List<PhysicalOpNode> list) {
        PhysicalOpType type = node.GetOpType();
        // 需要针对union node做特殊处理
        if (type.swigValue() == PhysicalOpType.kPhysicalOpRequestUnion.swigValue()) {
            PhysicalRequestUnionNode castNode = PhysicalRequestUnionNode.CastFrom(node);
            for (int i = 0; i < castNode.window_unions().GetSize(); i++) {
                dagToList(castNode.window_unions().GetKey(i), list);
            }
        }

        for (long i = 0; i < node.GetProducerCnt(); i++) {
            dagToList(node.GetProducer(i), list);
        }
        list.add(node);
    }

    public static TypeOuterClass.Type parquetTypeToFedbType(Type parquetPype) throws Exception {
        switch(parquetPype.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32:
                return TypeOuterClass.Type.kInt32;
            case INT64:
                return TypeOuterClass.Type.kInt64;
            case INT96:
                // TODO: Support int96 in parquet
                return TypeOuterClass.Type.kInt64;
            case FLOAT:
                return TypeOuterClass.Type.kFloat;
            case DOUBLE:
                return TypeOuterClass.Type.kDouble;
            case BOOLEAN:
                return TypeOuterClass.Type.kBool;
            case BINARY:
                return TypeOuterClass.Type.kVarchar;
            default:
                // TODO: Support timestamp and date
                throw new Exception("Get unsupported parquet type: " + parquetPype);
        }
    }

    public static TypeOuterClass.Type getFesqlType(String type) {
        if (type.equalsIgnoreCase("bigint") || type.equalsIgnoreCase("long")) {
            return TypeOuterClass.Type.kInt64;
        }
        if (type.equalsIgnoreCase("smallint") || type.equalsIgnoreCase("small") || type.equalsIgnoreCase("short")) {
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
        logger.error("fesql can't get this type {}", type);
        return null;
    }

    public static String getDDLType(TypeOuterClass.Type type) {
        if (TypeOuterClass.Type.kInt64 == type) {
            return "bigint";
        }
        if (TypeOuterClass.Type.kInt16 == type) {
            return "smallint";
        }
        if (TypeOuterClass.Type.kInt32 == type) {
            return "int";
        }
        if (TypeOuterClass.Type.kFloat == type) {
            return "float";
        }
        if (TypeOuterClass.Type.kDouble == type) {
            return "double";
        }
        if (TypeOuterClass.Type.kVarchar == type) {
            return "string";
        }
        if (TypeOuterClass.Type.kBool == type) {
            return "bool";
        }
        if (TypeOuterClass.Type.kTimestamp == type) {
            return "timestamp";
        }
        if (TypeOuterClass.Type.kDate == type) {
            return "date";
        }
        logger.error("fesql can't get this type {}", type);
        return null;
    }

    public static String getRtidbIndexType(TTLType type) {
        if (TTLType.kAbsAndLat == type) {
            return "absandlat";
        }
        if (TTLType.kAbsolute == type) {
            return "absolute";
        }
        if (TTLType.kLatest == type) {
            return "latest";
        }
        if (TTLType.kAbsOrLat == type) {
            return "absorlat";
        }
        return null;

    }

    public static List<TypeOuterClass.TableDef> getTableDefs(String jsonObject) {
        List<TypeOuterClass.TableDef> tableDefs = new ArrayList<>();
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

    public static List<String> addEscapeChar(List<String> list, String singleChar) {
        List<String> newList = new ArrayList<>();
        for (String e : list) {
            String str = String.format("%s%s%s", singleChar, e, singleChar);
            newList.add(str);
        }
        return newList;
    }

    public static String sql2Feconfig(String sql, String schema) {
        String tempDB = "temp_" + System.currentTimeMillis();
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName(tempDB);
        List<TypeOuterClass.TableDef> tables = getTableDefs(schema);
        Map<String, TypeOuterClass.TableDef> tableDefMap = new HashMap<>();
        for (TypeOuterClass.TableDef e : tables) {
            db.addTables(e);
            tableDefMap.put(e.getName(), e);
        }
        RequestEngine engine = null;
        try {
            engine = new RequestEngine(sql, db.build());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
        }
        PhysicalOpNode plan = engine.getPlan();
        return parseOpSchema(plan);

    }

    public static String sql2Feconfig(String sql, TypeOuterClass.Database db) {
        if (sql == null || sql.isEmpty()) {
            logger.error("sql script is null or empty, so return null");
            return null;
        }
        RequestEngine engine = null;
        try {
            engine = new RequestEngine(sql, db);
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
        }
        PhysicalOpNode plan = engine.getPlan();
        return parseOpSchema(plan);

    }

    public static String parseOpSchema(PhysicalOpNode plan) {
        List<Pair<String, String>> schemaPair = new ArrayList<>();

        for (TypeOuterClass.ColumnDef e : plan.GetOutputSchema()) {
            Pair<String, String> field = new Pair<>(e.getName(), getDDLType(e.getType()));
            schemaPair.add(field);
        }

        HashMap<String, List<Pair<String, String>>> feConfig = new HashMap<>();

        feConfig.put(SQLTableName, schemaPair);

        Gson gson = new Gson();
        String jsonConfig = String.format("{\"tableInfo\": %s}", gson.toJson(feConfig));
        return jsonConfig;

    }
}

@Data
class RtidbTable {
    String tableName;
    TypeOuterClass.TableDef schema;
    Set<RtidbIndex> indexs = new LinkedHashSet<>();

    // 需要考虑重复的index，并且找到范围最大ttl值
    public void addIndex(RtidbIndex index) {
        boolean flag = true;
        for (RtidbIndex e : indexs) {
            if (index.equals(e)) {
                flag = false;
                if (index.getAtmost() > e.getAtmost()) {
                    e.setAtmost(index.getAtmost());
                }
                if (index.getExpire() > e.getExpire()) {
                    e.setExpire(index.getExpire());
                }
                if (e.getType() == TTLType.kAbsAndLat || index.getType() == TTLType.kAbsAndLat || index.getType() != e.getType()) {
                    index.setType(TTLType.kAbsAndLat);
                    e.setType(TTLType.kAbsAndLat);
                }
                break;
            }
        }
        if (flag) {
            indexs.add(index);
        }
    }

    public void expandDDL() {
        //
        if (indexs.size() == 0) {
            for (TypeOuterClass.ColumnDef e : schema.getColumnsList()) {
                if (e.getType() == TypeOuterClass.Type.kVarchar || e.getType() == TypeOuterClass.Type.kInt32 || e.getType() == TypeOuterClass.Type.kInt64) {
                    RtidbIndex index = new RtidbIndex();
                    index.getKeys().add(e.getName());
                    index.setAtmost(1);
                    index.setType(TTLType.kLatest);
                    indexs.add(index);
                    break;
                }
            }
        }
    }

    public String toDDL() {
        expandDDL();

        StringBuilder str = new StringBuilder();
        str.append("create table ");
        String newTableName = String.format("`%s`", tableName);
        str.append(newTableName);
        str.append("(\n");
        for (int i = 0; i < schema.getColumnsCount(); i++) {
            String name = String.format("`%s`", schema.getColumns(i).getName());
            str.append(name);
            str.append(" ");
            str.append(DDLEngine.getDDLType(schema.getColumns(i).getType()));
            str.append(",\n");
        }
        List<String> indexsList = new ArrayList<>();
        for (RtidbIndex e : indexs) {
            indexsList.add(e.toIndexString());
        }
        str.append(StringUtils.join(indexsList, ",\n"));
        str.append("\n)");
        return str.toString();
    }
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
    // 因为fesql支持任意范围的窗口，所以需要kAbsAndLat这个类型。确保窗口中本该有数据，而没有被淘汰出去
    private TTLType type = TTLType.kAbsAndLat;
    // 映射到ritdb是最多保留多少条数据，不是最少
    private long atmost = 0;
    // 毫秒单位
    private long expire = 0;
    public String toIndexString() {
        List<String> newKeys = DDLEngine.addEscapeChar(keys, "`");
        String key = StringUtils.join(newKeys, ",");
        String ttlType = DDLEngine.getRtidbIndexType(type);
        String index = "";
        if (ts.equals("")) {
            index = String.format("index(key=(%s), ttl=%s, ttl_type=%s)", key, getTTL(), ttlType);
        } else {
            index = String.format("index(key=(%s), ts=`%s`, ttl=%s, ttl_type=%s)", key, ts, getTTL(), ttlType);
        }
        return index;
    }

    public String getTTL() {
        if (TTLType.kAbsAndLat == type) {
            long expireStr = 1L;
            if (expire % (60 * 1000) == 0) {
                expireStr = expire / (60 * 1000);
            } else {
                expireStr += expire / (60 * 1000);
            }
            return "(" + expireStr + "m, " + atmost + ")";
        }
        if (TTLType.kAbsolute == type) {
            long expireStr = 1L;
            if (expire % (60 * 1000) == 0) {
                expireStr = expire / (60 * 1000);
            } else {
                expireStr += expire / (60 * 1000);
            }
            return expireStr + "m";
        }
        if (TTLType.kLatest == type) {
            return String.valueOf(atmost);
        }
        if (TTLType.kAbsOrLat == type) {
            long expireStr = 1L;
            if (expire % (60 * 1000) == 0) {
                expireStr = expire / (60 * 1000);
            } else {
                expireStr += expire / (60 * 1000);
            }
            return "(" + expireStr + "m, " + atmost + ")";
        }
        return null;
    }
    //    @Override
    public boolean equals(RtidbIndex e) {
        return  this.getKeys().equals(e.getKeys()) && this.ts.equals(e.getTs());
    }
}

@Data
class Pair<K, V> {
    private K name;
    private V type;
    public Pair(K k, V v) {
        name = k;
        type = v;
    }
}
