package com._4paradigm.openmldb.test_common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class OpenMLDBDistribution implements Serializable {
    private String leader;
    private List<String> followers = new ArrayList<>();
}
