package com._4paradigm.openmldb.memoryusagecompare;

public class ResourceUsage {
    public String label;
    public long memoryUsage;

    public ResourceUsage(String label, long memoryUsage) {
        this.label = label;
        this.memoryUsage = memoryUsage;
    }
}