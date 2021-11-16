package com._4paradigm.openmldb.test_common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class OpenmldbDeployment implements Serializable {
    private String dbName;
    private String name;
    private String sql;
    private List<String> inColumns;
    private List<String> outColumns;
}
