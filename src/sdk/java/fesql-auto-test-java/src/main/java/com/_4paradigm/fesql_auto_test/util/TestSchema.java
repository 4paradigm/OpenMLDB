package com._4paradigm.fesql_auto_test.util;

import com._4paradigm.sql.DataType;
import com._4paradigm.sql.jdbc.SQLResultSetMetaData;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class TestSchema {
    List<Column> schema;
    public TestSchema(ResultSetMetaData meta) {
        schema = new ArrayList<>();
        if (meta instanceof SQLResultSetMetaData) {
            SQLResultSetMetaData sqlMetaData = (SQLResultSetMetaData)meta;
            try {
                for (int i = 0; i < sqlMetaData.getColumnCount(); i++) {
                    schema.add(new Column(sqlMetaData.getColumnName(i + 1), sqlMetaData.getColumnType(i + 1)));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int getColumnCount() {
        return schema.size();
    }

    public String getColumnName(int idx) {
        idx--;
        if (idx < schema.size() && idx >= 0) {
            return schema.get(idx).GetName();
        }
        return "";
    }

    public int getColumnType(int idx) {
        idx--;
        if (idx < schema.size() && idx >= 0) {
            return schema.get(idx).GetType();
        }
        return 0;
    }

    class Column {
        String name;
        int type;
        public Column(String name, int type) {
            this.name = name;
            this.type = type;
        }
        public String GetName() {
            return name;
        }
        public int GetType() {
            return type;
        }
    }
}
