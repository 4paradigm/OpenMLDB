package com._4paradigm.openmldb.stability;

import java.util.*;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Common;

public class TableInfo {
    private String db;
    private String name;
    private NS.TableInfo tableInfo;
    private List<String> columns;
    private Set<String> tsCol;

    public TableInfo(NS.TableInfo tableInfo) {
        this.db = tableInfo.getDb();
        this.name = tableInfo.getName();
        this.tableInfo = tableInfo;
        tsCol = new HashSet<>();
        for (Common.ColumnKey columnKey : tableInfo.getColumnKeyList()) {
            if (columnKey.hasTsName()) {
                tsCol.add(columnKey.getTsName());
            }
        }
    }

    public boolean isTsCol (String colName) {
        return tsCol.contains(colName);
    }

    public String getName() {
        return name;
    }

    public List<Common.ColumnDesc> getSchema() {
        return tableInfo.getColumnDescList();
    }
}
