package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;

import java.util.List;

@Data
public class SQLCase {
    int id;
    String desc;
    String mode;
    String db;
    String sql;
    List<String> sqls;
    boolean standard_sql;
    String create;
    String insert;
    List<String> tags;
    String batch_plan;
    String request_plan;
    List<InputDesc> inputs;
    OutputDesc expect;
}
