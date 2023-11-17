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

package com._4paradigm.openmldb.sdk;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com._4paradigm.openmldb.sdk.impl.Util;

public class Schema {
    private List<Column> columnList;
    private int size;

    public Schema(List<Column> columnList) {
        this.columnList = columnList;
        this.size = columnList.size();
    }

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    public String toString() {
        return columnList.stream().map(t -> {
            try {
                return t.getColumnName() + ":" + Util.sqlTypeToString(t.getSqlType());
            } catch (SQLException e) {
                return t.getColumnName() + ":unknown";
            }
        }).collect(Collectors.joining(","));
    }

    public int size() {
        return size;
    }

    public String getColumnName(int idx) {
        return columnList.get(idx).getColumnName();
    }

    public int getColumnType(int idx) {
        return columnList.get(idx).getSqlType();
    }

    public boolean isNullable(int idx) {
        return !columnList.get(idx).isNotNull();
    }
}
