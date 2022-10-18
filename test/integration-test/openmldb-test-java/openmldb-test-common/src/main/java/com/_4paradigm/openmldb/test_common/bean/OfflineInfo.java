package com._4paradigm.openmldb.test_common.bean;

import lombok.Data;

@Data
public class OfflineInfo {
    private String path;
    private String format;
    private boolean deepCopy;
    private String option;
}
