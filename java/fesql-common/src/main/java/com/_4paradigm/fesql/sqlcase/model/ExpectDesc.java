package com._4paradigm.fesql.sqlcase.model;

import lombok.Data;

@Data
public class ExpectDesc extends Table {
    String order;
    String count;
    Boolean success = true;
}
