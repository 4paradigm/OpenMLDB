

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

package com._4paradigm.openmldb.sdk.utils;

import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.DAGNode;
import com._4paradigm.openmldb.sdk.Schema;

import com.google.gson.Gson;

import java.sql.SQLException;
import java.sql.Types;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Map;

public class AIOSUtil {
    private static class AIOSDAGNode {
        public String uuid;
        public String script;
        public ArrayList<String> parents = new ArrayList<>();
        public ArrayList<String> inputTables = new ArrayList<>();
        public Map<String, String> tableNameMap = new HashMap<>();
    }
    
    private static class AIOSDAGColumn {
        public String name;
        public String type;
    }

    private static class AIOSDAGSchema {
        public String prn;
        public List<AIOSDAGColumn> cols = new ArrayList<>();
    }

    private static class AIOSDAG {
        public List<AIOSDAGNode> nodes = new ArrayList<>();
        public List<AIOSDAGSchema> schemas = new ArrayList<>();
    }

    private static int parseType(String type) {
        switch (type.toLowerCase()) {
            case "smallint":
            case "int16":
                return Types.SMALLINT;
            case "int32":
            case "i32":
            case "int":
                return Types.INTEGER;
            case "int64":
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.FLOAT;
            case "double":
                return Types.DOUBLE;
            case "bool":
            case "boolean":
                return Types.BOOLEAN;
            case "string":
                return Types.VARCHAR;
            case "timestamp":
                return Types.TIMESTAMP;
            case "date":
                return Types.DATE;
            default:
                throw new RuntimeException("Unknown type: " + type);
        }
    }

    private static DAGNode buildAIOSDAG(Map<String, String> sqls, Map<String, Map<String, String>> dag) {
        Queue<String> queue = new LinkedList<>();
        Map<String, List<String>> childrenMap = new HashMap<>();
        Map<String, Integer> degreeMap = new HashMap<>();
        Map<String, DAGNode> nodeMap = new HashMap<>();
        for (String uuid: sqls.keySet()) {
            Map<String, String> parents = dag.get(uuid);
            int degree = 0;
            if (parents != null) {
                for (String parent : parents.values()) {
                    if (dag.get(parent) != null) {
                        degree += 1;
                        if (childrenMap.get(parent) == null) {
                            childrenMap.put(parent, new ArrayList<>());
                        }
                        childrenMap.get(parent).add(uuid);
                    }
                }
            }
            degreeMap.put(uuid, degree);
            if (degree == 0) {
                queue.offer(uuid);
            }
        }

        ArrayList<DAGNode> targets = new ArrayList<>();
        while (!queue.isEmpty()) {
            String uuid = queue.poll();
            String sql = sqls.get(uuid);
            if (sql == null) {
                continue;
            }
            
            DAGNode node = new DAGNode(uuid, sql, new ArrayList<DAGNode>());
            Map<String, String> parents = dag.get(uuid);
            for (Map.Entry<String, String> parent : parents.entrySet()) {
                DAGNode producer = nodeMap.get(parent.getValue());
                if (producer != null) {
                    node.producers.add(new DAGNode(parent.getKey(), producer.sql, producer.producers));
                }
            }
            nodeMap.put(uuid, node);
            List<String> children = childrenMap.get(uuid);
            if (children == null || children.size() == 0) {
                targets.add(node);
            } else {
                for (String child : children) {
                    degreeMap.put(child, degreeMap.get(child) - 1);
                    if (degreeMap.get(child) == 0) {
                        queue.offer(child);
                    }
                }
            }
        }
        
        if (targets.size() == 0) {
            throw new RuntimeException("Invalid DAG: target node not found");
        } else if (targets.size() > 1) {
            throw new RuntimeException("Invalid DAG: target node is not unique");
        }
        return targets.get(0);
    }

    public static DAGNode parseAIOSDAG(String json) throws SQLException {
        Gson gson = new Gson();
        AIOSDAG graph = gson.fromJson(json, AIOSDAG.class);
        Map<String, String> sqls = new HashMap<>();
        Map<String, Map<String, String>> dag = new HashMap<>();

        for (AIOSDAGNode node : graph.nodes) {
            if (sqls.get(node.uuid) != null) {
                throw new RuntimeException("Duplicate 'uuid': " + node.uuid);
            }
            if (node.parents.size() != node.inputTables.size()) {
                throw new RuntimeException("Size of 'parents' and 'inputTables' mismatch: " + node.uuid);
            }
            Map<String, String> parents = new HashMap<String, String>();
            for (int i = 0; i < node.parents.size(); i++) {
                String table = node.inputTables.get(i);
                if (parents.get(table) != null) {
                    throw new RuntimeException("Ambiguous name '" + table +  "': " + node.uuid);
                }
                parents.put(table, node.parents.get(i));
            }
            sqls.put(node.uuid, node.script);
            dag.put(node.uuid, parents);
        }
        return buildAIOSDAG(sqls, dag);
    }

    public static Map<String, Map<String, Schema>> parseAIOSTableSchema(String json, String usedDB) {
        Gson gson = new Gson();
        AIOSDAG graph = gson.fromJson(json, AIOSDAG.class);
        Map<String, String> sqls = new HashMap<>();
        for (AIOSDAGNode node : graph.nodes) {
            sqls.put(node.uuid, node.script);
        }
        
        Map<String, Schema> schemaMap = new HashMap<>();
        for (AIOSDAGSchema schema : graph.schemas) {
            List<Column> columns = new ArrayList<>();
            for (AIOSDAGColumn column : schema.cols) {
                try {
                    columns.add(new Column(column.name, parseType(column.type)));
                } catch (Exception e) {
                    throw new RuntimeException("Unknown SQL type: " + column.type);
                }
            }
            schemaMap.put(schema.prn, new Schema(columns));
        }
        
        Map<String, Schema> tableSchema0 = new HashMap<>();
        for (AIOSDAGNode node : graph.nodes) {
            for (int i = 0; i < node.parents.size(); i++) {
                String table = node.inputTables.get(i);
                if (sqls.get(node.parents.get(i)) == null) {
                    String prn = node.tableNameMap.get(table);
                    if (prn == null) {
                        throw new RuntimeException("Table not found in 'tableNameMap': " +
                            node.uuid + " " + table);
                    }
                    Schema schema = schemaMap.get(prn);
                    if (schema == null) {
                        throw new RuntimeException("Schema not found: " + prn);
                    }
                    if (tableSchema0.get(table) != null) {
                        if (tableSchema0.get(table) != schema) {
                            throw new RuntimeException("Table name conflict: " + table);
                        }
                    }
                    tableSchema0.put(table, schema);
                }
            }
        }

        Map<String, Map<String, Schema>> tableSchema = new HashMap<>();
        tableSchema.put(usedDB, tableSchema0);
        return tableSchema;
    }
}