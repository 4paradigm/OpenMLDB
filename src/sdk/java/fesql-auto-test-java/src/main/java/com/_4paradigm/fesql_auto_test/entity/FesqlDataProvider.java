package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.CaseFile;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.collections.Lists;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 3:19 PM
 */
@Data
public class FesqlDataProvider extends CaseFile {
    private static Logger logger = LoggerFactory.getLogger(FesqlDataProvider.class);
    public static final String FAIL_SQL_CASE= "FailSQLCase";

    public static FesqlDataProvider dataProviderGenerator(String caseFile) throws FileNotFoundException {
        try {
            Yaml yaml = new Yaml();
            FileInputStream testDataStream = new FileInputStream(caseFile);
            FesqlDataProvider testDateProvider = yaml.loadAs(testDataStream, FesqlDataProvider.class);
            return testDateProvider;
        } catch (Exception e) {
            logger.error("fail to load yaml: ", e);
            FesqlDataProvider nullDataProvider = new FesqlDataProvider();
            SQLCase failCase = new SQLCase();
            failCase.setDesc(FAIL_SQL_CASE);
            nullDataProvider.setCases(Lists.newArrayList(failCase));
            return nullDataProvider;
        }
    }


}

