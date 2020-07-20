package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;

@Data
public class ExpectDesc extends Table {
    String order;
    int count = -1;
    Boolean success = true;
}
