package com._4paradigm.openmldb.java_sdk_test.entity;

import lombok.Data;

import java.util.List;

@Data
public class OpenmldbDeployment2 {
    private String dbName;
    private String name;
    private String sql;
    private List<OpenMLDBProcedureColumn2> inColumns;
    private List<OpenMLDBProcedureColumn2> outColumns;
}
