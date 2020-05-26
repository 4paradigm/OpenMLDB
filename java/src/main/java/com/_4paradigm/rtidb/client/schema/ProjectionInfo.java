package com._4paradigm.rtidb.client.schema;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class ProjectionInfo {
    private int formatVersion;
    private int maxIndex;
    private List<Integer> cols;
    private BitSet bset;
    private List<ColumnDesc> schema;

    public ProjectionInfo(List<Integer> cols, BitSet bset, int maxIndex) {
        this.cols = cols;
        this.bset = bset;
        this.maxIndex = maxIndex;
        this.formatVersion = 0;
    }

    public ProjectionInfo(List<ColumnDesc> schema) {
        this.schema = schema;
        this.formatVersion = 1;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public List<Integer> getProjectionCol() {
        return cols;
    }

    public BitSet getBitSet() {
        return bset;
    }

    public int getMaxIndex() {
        return maxIndex;
    }
}
