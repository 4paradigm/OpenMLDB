package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;
import java.util.List;
@Data
public class InputDesc {
    String name;
    String index;
    String schema;
    String data;
    String resource;
    List<String> indexs;
    List<String> columns;
    List<List<String>> rows;
}
