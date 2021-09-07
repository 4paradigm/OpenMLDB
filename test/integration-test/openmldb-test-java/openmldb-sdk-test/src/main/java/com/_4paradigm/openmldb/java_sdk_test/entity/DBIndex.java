package com._4paradigm.openmldb.java_sdk_test.entity;

import lombok.Data;

import java.util.List;

@Data
public class DBIndex {
    private int id;
    private String indexName;
    private List<String> keys;
    private String ts;
    private String ttl;
    private String ttlType;
}
