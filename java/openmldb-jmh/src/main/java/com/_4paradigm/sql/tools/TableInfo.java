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

package com._4paradigm.sql.tools;

import com._4paradigm.sql.BenchmarkConfig;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class TableInfo {
    private String name;
    private String ddl;
    private Map<String, Integer> schemaPos;
    private Map<Integer, String> schemaPosName;
    private List<String> columns;
    private List<String> indexs;
    private List<String> schema;
    private Set<Integer> tsIndex;
    private Set<Integer> index;
    private Map<Integer, String> colRelation;

    public TableInfo(String ddl, Relation relation) {
        if (ddl.contains("replicanum=")) {
            this.ddl = ddl + ", partitionnum=" + BenchmarkConfig.PARTITION_NUM + ";";
        } else {
            this.ddl = ddl + " OPTIONS(partitionnum=" + BenchmarkConfig.PARTITION_NUM + ");";
        }
        String[] arr = ddl.split("index\\(")[0].split("\\(");
        name = arr[0].split(" ")[2].replaceAll("`", "");
        String[] filed = arr[1].split(",");
        schema = new ArrayList<>();
        columns = new ArrayList<>();
        schemaPos = new HashMap<>();
        schemaPosName = new HashMap<>();
        for (int i = 0; i < filed.length; i++) {
            String[] tmp = filed[i].split(" ");
            if (tmp.length < 2) {
                continue;
            }
            columns.add(tmp[0] + " " + tmp[1]);
            schema.add(tmp[1].trim());
            String fieldName = tmp[0].replaceAll("`", "");
            schemaPos.put(fieldName, i);
            schemaPosName.put(i, fieldName);
        }
        parseRelation(relation);
    }

    public TableInfo(String name, JSONArray jsonSchema, Relation relation) {
        this.name = name;
        schema = new ArrayList<>();
        schemaPos = new HashMap<>();
        schemaPosName = new HashMap<>();
        try {
            for (int i = 0; i < jsonSchema.length(); i++) {
                JSONObject val = jsonSchema.getJSONObject(i);
                String field = val.getString("name");
                String type = val.getString("type");
                schema.add(type);
                schemaPos.put(field, i);
                schemaPosName.put(i, field);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        parseRelation(relation);
    }

    public String getTypeString() {
        StringBuilder stringBuilder  = new StringBuilder();
        for (int i = 0; i < schema.size(); i++) {
            if (i > 0) {
                stringBuilder.append(",");
            }
            String name = schemaPosName.get(i);
            stringBuilder.append("`");
            stringBuilder.append(name);
            stringBuilder.append("` ");
            if (schema.get(i).equals("int")) {
                stringBuilder.append("int32");
            } else {
                stringBuilder.append(schema.get(i));
            }
        }
        return stringBuilder.toString();
    }

    private void parseRelation(Relation relation) {
        String indexStr = relation.getIndex().get(name);
        String tsIndexStr = relation.getTsIndex().get(name);
        index = new HashSet<>();
        for (String val : indexStr.trim().split(",")) {
            String[] tmp = val.split("\\|");
            for (String field : tmp) {
                index.add(schemaPos.get(field));
            }
        }
        tsIndex = new HashSet<>();
        if (!tsIndexStr.equals("null")) {
            for (String val : tsIndexStr.trim().split(",")) {
                tsIndex.add(schemaPos.get(val));
            }
        }
        String relationStr = relation.getColRelaion().get(name);
        colRelation = new HashMap<>();
        if (!relationStr.equals("null")) {
            for (String val : relationStr.trim().split(",")) {
                String[] tmp = val.split("\\|");
                if (tmp.length == 2) {
                    colRelation.put(schemaPos.get(tmp[1]), tmp[0]);
                }
            }
        }
        String mainTable = relation.getMainTable();
        if (name.equals(mainTable)) {
            for (Map.Entry<String, String> entry : relation.getColRelaion().entrySet()) {
                if (entry.getKey().equals(mainTable)) {
                    continue;
                }
                String curRelationStr = entry.getValue();
                String[] tmp = curRelationStr.trim().split("\\|");
                index.add(schemaPos.get(tmp[0]));
            }
        }
    }

    public String getDDL() { return ddl; }
    public Set<Integer> getTsIndex() { return tsIndex; }
    public String getName() { return name; }
    public Set<Integer> getIndex() { return index; }
    public List<String> getSchema() { return schema; }
    public List<String> getColumns() { return columns; }
    public List<String> getIndexs() { return indexs; }

    public Map<String, Integer> getSchemaPos() { return schemaPos; }
    public Map<Integer, String> getColRelation() { return colRelation; }
    public Map<Integer, String> getSchemaPosName() { return schemaPosName; }
}
