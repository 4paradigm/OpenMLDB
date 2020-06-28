package com._4paradigm.fesql.sqlcase.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
public class CaseFile {
    String db;
    List<String> debugs;
    List<SQLCase> cases;
}
