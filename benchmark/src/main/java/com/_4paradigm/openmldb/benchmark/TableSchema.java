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

package com._4paradigm.openmldb.benchmark;

import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;

public class TableSchema {
    String dataBase;
    String tableName;
    List<Type.DataType> schema = new ArrayList<>();
    Set<Integer> index = new HashSet<>();
    Set<Integer> tsIndex = new HashSet<>();

    public TableSchema(NS.TableInfo tableInfo) {
        dataBase = tableInfo.getDb();
        tableName = tableInfo.getName();
        Map<String, Integer> fieldMap = new HashMap<>();
        for (int idx = 0; idx < tableInfo.getColumnDescCount(); idx++) {
            schema.add(tableInfo.getColumnDesc(idx).getDataType());
            fieldMap.put(tableInfo.getColumnDesc(idx).getName(), idx);
        }
        for (int idx = 0; idx < tableInfo.getColumnKeyCount(); idx++) {
            for (int i = 0; i < tableInfo.getColumnKey(idx).getColNameCount(); i++) {
                index.add(fieldMap.get(tableInfo.getColumnKey(idx).getColName(i)));
            }
            if (tableInfo.getColumnKey(idx).hasTsName()) {
                tsIndex.add(fieldMap.get(tableInfo.getColumnKey(idx).getTsName()));
            }
        }
    }

    public String getDataBase() {
        return dataBase;
    }

    public String getTableName() {
        return tableName;
    }

    public List<Type.DataType> getSchema() {
        return schema;
    }

    public Set<Integer> getIndex() {
        return index;
    }

    public Set<Integer> getTsIndex() {
        return tsIndex;
    }
}
