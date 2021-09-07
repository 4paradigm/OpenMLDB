package com._4paradigm.openmldb.test_common.restful.common;


import com._4paradigm.openmldb.test_common.restful.model.HttpMethod;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.restful.util.HttpRequest;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.Map;

@Data
@Slf4j
public class FedbHttp {
    private String url;
    private String uri;
    private String body;
    private Map<String, Object> data = new HashMap<>();
    private Boolean isJson;
    protected Map<String, String> headMap = new HashMap<>();
    private HttpMethod method;
    private Boolean isDebug = false;

    public void setHeadMap(Map<String, String> headMap) {
        if(MapUtils.isNotEmpty(headMap)) {
            log.info("添加了头：" + headMap);
            this.headMap = headMap;
        }
    }

    public void getDebug() {
        HttpRequest.debug = isDebug;
    }

    /**
     * 请求中参数都在url、body中都有参数
     * 请求的URI中有可变参数
     * 适应于GET、POST、PUT、DELETE方法使用
     *
     * @return
     */
    public HttpResult restfulRequest() {
        getDebug();
        String realUrl = this.url+this.uri;
        HttpResult result = null;
        if (isJson) {
            this.headMap.put("Content-Type",
                    "application/json;charset=utf-8");
        } else {
            this.headMap.put("Content-Type",
                    "application/x-www-form-urlencoded;charset=utf-8");
        }
        try {
            switch (this.method) {
                case GET:
                    result = HttpRequest.get(realUrl,null, this.headMap);
                    break;
                case POST:
                    if (isJson) {
                        result = HttpRequest.postJson(realUrl, body, this.headMap);
                    } else {
                        result = HttpRequest.post(realUrl, this.data, this.headMap);
                    }
                    break;
                case PUT:
                    if (isJson) {
                        result = HttpRequest.put(realUrl, body, this.headMap);
                    } else {
                        result = HttpRequest.put(realUrl, this.data, this.headMap);
                    }
                    break;
                case DELETE:
                    result = HttpRequest.delete(realUrl, this.data, this.headMap);
                    break;

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
