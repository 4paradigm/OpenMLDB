package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.type.TableType;

import java.util.List;

public class TableDesc {

    private String name;
    private TableType tableType;
    private List<ColumnDesc> columnDescList;
    private List<IndexDef> indexs;

    public TableDesc() {
    }

    public TableDesc(String name, TableType tableType, List<ColumnDesc> columnDescList, List<IndexDef> indexs) {
        this.name = name;
        this.tableType = tableType;
        this.columnDescList = columnDescList;
        this.indexs = indexs;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }

    public List<ColumnDesc> getColumnDescList() {
        return columnDescList;
    }

    public void setColumnDescList(List<ColumnDesc> columnDescList) {
        this.columnDescList = columnDescList;
    }

    public List<IndexDef> getIndexs() {
        return indexs;
    }

    public void setIndexs(List<IndexDef> indexs) {
        this.indexs = indexs;
    }
}
