package com._4paradigm.fesql.offline.sqlcase.model;

import lombok.Data;

import java.util.List;

@Data
public class SQLCase {
    int id;
    String desc;
    String mode;
    String db;
    String sql;
    String tag;
    List<InputDesc> inputs;
    OutputDesc output;
}
