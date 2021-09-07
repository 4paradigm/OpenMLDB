package com._4paradigm.openmldb.java_sdk_test.entity;

import lombok.Data;

import java.util.List;

@Data
public class DBSchema {
    private List<DBColumn> cols;
    private List<DBIndex> indexs;
}
