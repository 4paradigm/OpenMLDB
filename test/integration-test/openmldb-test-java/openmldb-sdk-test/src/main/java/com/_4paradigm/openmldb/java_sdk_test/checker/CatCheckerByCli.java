package com._4paradigm.openmldb.java_sdk_test.checker;

import com._4paradigm.openmldb.test_common.command.CommandUtil;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.CatFile;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;

@Slf4j
public class CatCheckerByCli extends BaseChecker{
    public CatCheckerByCli(ExpectDesc expect, OpenMLDBResult fesqlResult) {
        super(expect, fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("cat check");
        CatFile expectCat = expect.getCat();
        String path = expectCat.getPath();
        path = SQLUtil.formatSql(path, openMLDBResult.getTableNames());
        path = SQLUtil.formatSql(path);
        String command = "cat "+path;
        List<String> actualList = CommandUtil.run(command);
        List<String> expectList = expectCat.getLines();
        Assert.assertEquals(actualList,expectList);
    }
}
