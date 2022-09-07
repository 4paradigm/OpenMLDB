package com._4paradigm.openmldb.test_common.common;

/**
 * Created by zhangguanglin on 2020/1/16.
 */
@FunctionalInterface
public interface Condition {
    Boolean execute();
}
