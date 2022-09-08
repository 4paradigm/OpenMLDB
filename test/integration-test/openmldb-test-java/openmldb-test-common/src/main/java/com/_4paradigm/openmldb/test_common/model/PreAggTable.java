package com._4paradigm.openmldb.test_common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class PreAggTable implements Serializable {
    private String name;
    private String type;
    private int count = -1;
    private List<List<Object>> rows;
}
