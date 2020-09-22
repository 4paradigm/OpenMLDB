package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.Table;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.Schema;
import com.google.common.base.Joiner;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class ResultChecker extends BaseChecker {

    private static final Logger logger = LoggerFactory.getLogger(ResultChecker.class);

    public ResultChecker(SQLCase fesqlCase, FesqlResult fesqlResult) {
        super(fesqlCase, fesqlResult);
    }

    @Override
    public void check() throws ParseException {
        log.info("result check");
        if (fesqlCase.getExpect().getColumns().isEmpty()) {
            throw new RuntimeException("fail check result: columns are empty");
        }
        List<List<Object>> expect = FesqlUtil.convertRows(fesqlCase.getExpect().getRows(),
                fesqlCase.getExpect().getColumns());
        List<List<Object>> actual = fesqlResult.getResult();

        String orderName = fesqlCase.getExpect().getOrder();
        if (orderName != null && orderName.length() > 0) {
            Schema schema = fesqlResult.getResultSchema();
            int index = 0;
            if (schema != null) {
                index = FesqlUtil.getIndexByColumnName(schema, orderName);
            } else {
                index = FesqlUtil.getIndexByColumnName(fesqlResult.getMetaData(), orderName);
            }
            Collections.sort(expect, new RowsSort(index));
            Collections.sort(actual, new RowsSort(index));
        }

        log.info("expect:{}", expect);
        log.info("actual:{}", actual);
        Assert.assertEquals(actual.size(), expect.size(),
                String.format("ResultChecker fail: expect size %d, real size %d", expect.size(), actual.size()));
        Assert.assertEquals(actual, expect,
                String.format("ResultChecker fail: expect\n%s\nreal\n%s", Table.getTableString(fesqlCase.getExpect().getColumns(), expect), fesqlResult.toString()));
    }

    public class RowsSort implements Comparator<List> {
        private int index;

        public RowsSort(int index) {
            this.index = index;
        }

        @Override
        public int compare(List o1, List o2) {
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

}
