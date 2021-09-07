package com._4paradigm.openmldb.test_common.restful.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class Expect implements Serializable {
    private String code;
    private String msg;
    private Map<String,Object> data;
    private List<String> columns;
    private List<List<Object>> rows;
    private List<String> sqls;
}
