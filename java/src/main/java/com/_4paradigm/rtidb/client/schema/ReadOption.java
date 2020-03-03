package com._4paradigm.rtidb.client.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReadOption {

    private Map<String, Object> index;
    private List<ReadFilter> readFilter;
    private Set<String> colSet;
    private int limit;

    public ReadOption(Map<String, Object> index, List<ReadFilter> readFilter, Set<String> colSet, int limit) {
        this.index = index;
        this.readFilter = readFilter;
        this.colSet = colSet;
        this.limit = limit;
    }

    public Map<String, Object> getIndex() {
        return index;
    }

    public void setIndex(Map<String, Object> index) {
        this.index = index;
    }

    public List<ReadFilter> getReadFilter() {
        return readFilter;
    }

    public void setReadFilter(List<ReadFilter> readFilter) {
        this.readFilter = readFilter;
    }

    public Set<String> getColSet() {
        return colSet;
    }

    public void setColSet(Set<String> colSet) {
        this.colSet = colSet;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }
}
