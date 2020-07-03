package com._4paradigm.fesql_auto_test.checker;

import com._4paradigm.fesql_auto_test.entity.FesqlCase;
import com._4paradigm.fesql_auto_test.entity.FesqlResult;
import com._4paradigm.fesql_auto_test.util.FesqlUtil;
import com._4paradigm.sql.Schema;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/22 4:56 PM
 */
@Slf4j
public class ColumnChecker extends BaseChecker {
    public ColumnChecker(FesqlCase fesqlCase, FesqlResult fesqlResult){
        super(fesqlCase,fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("column check");
        List<String> expect =  (List<String>)fesqlCase.getExpect().get("columns");
        Schema schema = fesqlResult.getResultSchema();
        Assert.assertEquals(schema.GetColumnCnt(),expect.size());
        for(int i=0;i<expect.size();i++){
            Assert.assertEquals(schema.GetColumnName(i)+" "+ FesqlUtil.getColumnType(schema.GetColumnType(i)),expect.get(i));
        }
    }
}
