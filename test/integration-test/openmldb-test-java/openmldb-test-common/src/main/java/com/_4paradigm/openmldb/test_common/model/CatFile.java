package com._4paradigm.openmldb.test_common.model;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class CatFile implements Serializable {
    private String path;
    private List<String> lines;
}
