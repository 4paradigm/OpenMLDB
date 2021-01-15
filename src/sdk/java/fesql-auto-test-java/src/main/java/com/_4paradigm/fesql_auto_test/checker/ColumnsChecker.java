package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql.sqlcase.model.Table;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.fesql_auto_test.util.TestSchema;
import com._4paradigm.sql.Schema;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.sql.ResultSetMetaData;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/16 3:14 PM
 */
@Slf4j
public class ColumnsChecker extends BaseChecker {

    public ColumnsChecker(SQLCase fesqlCase, FesqlResult fesqlResult) {
        super(fesqlCase, fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("column name check");
        List<String> expect = (List<String>) fesqlCase.getExpect().getColumns();
        if (expect == null || expect.size() == 0) {
            return;
        }
        Schema schema = fesqlResult.getResultSchema();
        if (schema != null) {
            Assert.assertEquals(expect.size(), schema.GetColumnCnt(), "Illegal schema size");
            for (int i = 0; i < expect.size(); i++) {
                Assert.assertEquals(schema.GetColumnName(i), Table.getColumnName(expect.get(i)));
                Assert.assertEquals(schema.GetColumnType(i),
                        FesqlUtil.getColumnType(Table.getColumnType(expect.get(i))));
            }
        } else {
            TestSchema schemaMeta = fesqlResult.getMetaSchema();
            Assert.assertEquals(expect.size(), schemaMeta.getColumnCount(), "Illegal schema size");
            for (int i = 0; i < expect.size(); i++) {
                Assert.assertEquals(schemaMeta.getColumnName(i + 1), Table.getColumnName(expect.get(i)));
                Assert.assertEquals(schemaMeta.getColumnType(i + 1),
                        FesqlUtil.getSQLType(Table.getColumnType(expect.get(i))));
            }
        }

    }
}
