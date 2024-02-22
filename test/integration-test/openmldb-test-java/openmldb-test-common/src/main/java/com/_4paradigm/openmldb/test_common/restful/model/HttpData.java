package com._4paradigm.openmldb.test_common.restful.model;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class HttpData {
    private Map<String,List<Object>> data;
    private Integer code;
    private String msg;

}
