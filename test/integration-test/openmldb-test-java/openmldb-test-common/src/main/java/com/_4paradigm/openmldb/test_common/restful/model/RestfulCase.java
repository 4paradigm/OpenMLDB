package com._4paradigm.openmldb.test_common.restful.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class RestfulCase implements Serializable {
    private String caseId;
    private String desc;
    private String module;
    private List<String> tags;
    private int level;

    private String uri;
    private String method;
    private Map<String,String> headers;
    private Map<String,List<String>> uriParameters;
    private Map<String,List<String>> bodyParameters;
    private String body;

    private BeforeAction beforeAction;
    private AfterAction afterAction;
    private AfterAction tearDown;
    private Expect expect;
    private List<Expect> uriExpect;
    private List<Expect> bodyExpect;

}
