package com._4paradigm.openmldb.java_sdk_test.entity;

import lombok.Data;

@Data
public class DBColumn {
    private int id;
    private String fieldName;
    private String fieldType;
    private boolean nullable;
}
