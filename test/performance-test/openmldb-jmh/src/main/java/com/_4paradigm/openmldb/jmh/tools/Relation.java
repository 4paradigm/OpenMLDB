package com._4paradigm.openmldb.jmh.tools;

import java.util.HashMap;
import java.util.Map;

public class Relation {
    private String mainTable;
    private Map<String, String> index;
    private Map<String, String> tsIndex;
    private Map<String, String> colRelation;

    public Relation(String relationStr) {
        String[] arr = relationStr.trim().split("\n");
        index = new HashMap<>();
        tsIndex = new HashMap<>();
        colRelation = new HashMap<>();
        for (String item : arr) {
            String[] tmp = item.trim().split(" ");
            if (tmp.length < 5) {
                System.out.println("parse relation error");
                continue;
            }
            String name = tmp[0];
            if (tmp[1].equals("null")) {
                mainTable = name;
            }
            colRelation.put(name, tmp[2]);
            index.put(name, tmp[3]);
            tsIndex.put(name, tmp[4]);
        }
    }
    public String getMainTable() { return mainTable; }
    public Map<String, String> getIndex() { return index; }
    public Map<String, String> getTsIndex() { return tsIndex; }
    public Map<String, String> getColRelaion() { return colRelation; }
}
