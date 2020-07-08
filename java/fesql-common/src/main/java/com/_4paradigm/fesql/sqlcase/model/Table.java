package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;

import java.util.List;

@Data
public class Table {
    String name;
    String index;
    String schema;
    String data;
    List<String> indexs;
    List<String> columns;
    List<List<String>> rows;
}
