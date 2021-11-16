package com._4paradigm.openmldb.java_sdk_test.checker;

import com._4paradigm.openmldb.java_sdk_test.command.CommandUtil;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import com._4paradigm.openmldb.test_common.model.CatFile;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.OpenmldbDeployment;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;

import java.util.List;

@Slf4j
public class CatCheckerByCli extends BaseChecker{
    public CatCheckerByCli(ExpectDesc expect, FesqlResult fesqlResult) {
        super(expect, fesqlResult);
    }

    @Override
    public void check() throws Exception {
        log.info("cat check");
        reportLog.info("cat check");
        CatFile expectCat = expect.getCat();
        String path = expectCat.getPath();
        path = FesqlUtil.formatSql(path, fesqlResult.getTableNames());
        String command = "cat "+path;
        List<String> actualList = CommandUtil.run(command);
        List<String> expectList = expectCat.getLines();
        Assert.assertEquals(actualList,expectList);
    }
}
