package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.CaseFile;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.util.Tool;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhaowei
 * @date 2020/6/11 3:19 PM
 */
@Data
public class FesqlDataProvider extends CaseFile {
    private static Logger logger = LoggerFactory.getLogger(FesqlDataProvider.class);

    public static FesqlDataProvider dataProviderGenerator(String caseFile) throws FileNotFoundException {
        Yaml yaml = new Yaml();
//        String rtidbDir = Tool.rtidbDir().getAbsolutePath();
//        Assert.assertNotNull(rtidbDir);
//        String caseAbsPath = rtidbDir + "/fesql/cases/" + caseFile;
//        logger.debug("fesql case absolute path: {}", caseAbsPath);
        FileInputStream testDataStream = new FileInputStream(caseFile);
        FesqlDataProvider testDateProvider = yaml.loadAs(testDataStream, FesqlDataProvider.class);
        return testDateProvider;
    }


}

