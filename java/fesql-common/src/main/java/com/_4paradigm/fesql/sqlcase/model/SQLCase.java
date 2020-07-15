package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;

import java.util.List;

@Data
public class SQLCase {
    String id;
    String desc;
    String mode;
    String db;
    String sql;
    List<String> sqls;
    boolean standard_sql;
    List<String> tags;
    String batch_plan;
    String request_plan;
    List<InputDesc> inputs;
    ExpectDesc expect;
}
