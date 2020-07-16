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
        List<List<Object>> expect = convertRows(fesqlCase.getExpect().getRows(),
                fesqlCase.getExpect().getColumns());
        List<List> actual = fesqlResult.getResult();

        String orderName = fesqlCase.getExpect().getOrder();
        if (orderName != null && orderName.length() > 0) {
            Schema schema = fesqlResult.getResultSchema();
            int index = FesqlUtil.getIndexByColumnName(schema, orderName);
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
            if (obj1 instanceof Comparable && obj2 instanceof Comparable) {
                return ((Comparable) obj1).compareTo(obj2);
            } else {
                return obj1.hashCode() - obj2.hashCode();
            }
        }
    }

    public static List<List<Object>> convertRows(List<List<Object>> rows, List<String> columns) throws ParseException {
        List<List<Object>> list = new ArrayList<>();
        for (List row : rows) {
            list.add(convertList(row, columns));
        }
        return list;
    }

    public static List<Object> convertList(List<Object> datas, List<String> columns) throws ParseException {
        List<Object> list = new ArrayList();
        for (int i = 0; i < datas.size(); i++) {
            if (datas.get(i) == null) {
                list.add(null);
            } else {
                String obj = datas.get(i).toString();
                String column = columns.get(i);
                list.add(convertData(obj, column));
            }
        }
        return list;
    }

    public static Object convertData(String data, String column) throws ParseException {
        String[] ss = column.split("\\s+");
        String type = ss[ss.length - 1];
        Object obj = null;
        if ("null".equalsIgnoreCase(data)) {
            return null;
        }
        switch (type) {
            case "smallint":
            case "int16":
                obj = Short.parseShort(data);
                break;
            case "int32":
            case "i32":
            case "int":
                obj = Integer.parseInt(data);
                break;
            case "int64":
            case "bigint":
                obj = Long.parseLong(data);
                break;
            case "float":
                obj = Float.parseFloat(data);
                break;
            case "double":
                obj = Double.parseDouble(data);
                break;
            case "bool":
                obj = Boolean.parseBoolean(data);
                break;
            case "string":
                obj = data;
                break;
            case "timestamp":
                obj = new Timestamp(Long.parseLong(data));
                break;
            case "date":
                try {
                    obj = new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data.trim() + " 00:00:00").getTime());
                } catch (ParseException e) {
                    logger.error("Fail convert {} to date", data.trim());
                    throw e;
                }
                break;
            default:
                obj = data;
                break;
        }
        return obj;
    }
}
