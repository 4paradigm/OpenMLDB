package com._4paradigm.openmldb.test_common.common;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by zhangguanglin on 2020/1/16.
 */
@FunctionalInterface
public interface ConditionResult<T> {
    Pair<Boolean,T> execute();
}
