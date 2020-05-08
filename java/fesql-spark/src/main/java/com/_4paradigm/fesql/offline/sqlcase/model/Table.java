package com._4paradigm.fesql.offline.sqlcase.model;

import lombok.Data;

@Data
public class Table {
    String name;
    String schema;
    String index;
    String data;
}
