package com._4paradigm.openmldb.java_sdk_test.checker;


import com._4paradigm.openmldb.test_common.common.ReportLog;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;

/**
 * @author zhaowei
 * @date 2021/3/10 6:35 PM
 */
@Slf4j
public class RowsSort implements Comparator<List> {
    private int index;
    private ReportLog reportLog = ReportLog.of();

    public RowsSort(int index) {
        this.index = index;
        if (-1 == index) {
            log.warn("compare without index");
            reportLog.warn("compare without index");
        }
    }

    @Override
    public int compare(List o1, List o2) {
        if (-1 == index) {

            return 0;
        }
        Object obj1 = o1.get(index);
        Object obj2 = o2.get(index);
        if (obj1 == obj2) {
            return 0;
        }
        if (obj1 == null) {
            return -1;
        }
        if (obj2 == null) {
            return 1;
        }
        if (obj1 instanceof Comparable && obj2 instanceof Comparable) {
            return ((Comparable) obj1).compareTo(obj2);
        } else {
            return obj1.hashCode() - obj2.hashCode();
        }
    }
}
