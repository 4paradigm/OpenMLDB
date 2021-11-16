package com._4paradigm.openmldb.test_common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class TableIndex implements Serializable {
    private String name;
    private List<String> keys;
    private String ts;
    private String ttl;
    private String ttlType;
}
