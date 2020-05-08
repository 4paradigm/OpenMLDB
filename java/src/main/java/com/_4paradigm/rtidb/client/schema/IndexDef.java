package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.type.IndexType;

import java.util.List;

public class IndexDef {

    private String indexName;
    private List<String> colNameList;
    private IndexType indexType;

    public IndexDef() {
    }

    public IndexDef(String indexName, List<String> colNameList, IndexType indexType) {
        this.indexName = indexName;
        this.colNameList = colNameList;
        this.indexType = indexType;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getColNameList() {
        return colNameList;
    }

    public void setColNameList(List<String> colNameList) {
        this.colNameList = colNameList;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }
}