package com._4paradigm.openmldb.test_common.restful.model;

import lombok.Data;
import org.apache.http.cookie.Cookie;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class HttpResult implements Serializable {
    private int httpCode;
    private Map<String,String> headers;
    private List<Cookie> cookies;
    private long beginTime;
    private long endTime;
    private String data;

}
