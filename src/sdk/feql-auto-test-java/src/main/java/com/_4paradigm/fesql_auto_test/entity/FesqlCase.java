package com._4paradigm.fesql_auto_test.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author zhaowei
 * @date 2020/6/11 3:25 PM
 */
@Data
public class FesqlCase {
    private long id;
    private String db;
    private String sql;
    private String check_sql;
    private List<String> sqls;
    private String desc;
    private List<FesqlCaseInput> inputs;
    private String executor;
    private List<String> tag;
    private boolean createDB = true;
    private Map<String,Object> expect;
}
