package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;
import java.util.List;
@Data
public class OutputDesc {
    String schema;
    String data;
    String order;
    String count;
    List<String> indexs;
    List<String> columns;
    List<List<String>> rows;
}
