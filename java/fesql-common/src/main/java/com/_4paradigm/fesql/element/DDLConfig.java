package com._4paradigm.fesql.element;

import lombok.Data;

@Data
public class DDLConfig {
    private String sqlScript;
    private String schemaJson;
    private int replicaNumber = 1;
    private int partitionNumber = 1;
    private boolean dropIfExist = false;
    private boolean isAlwaysExist = false;

}
