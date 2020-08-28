package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.CaseFile;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author zhaowei
 * @date 2020/6/11 3:19 PM
 */
@Data
public class FesqlDataProvider extends CaseFile {
    private static Logger logger = LoggerFactory.getLogger(FesqlDataProvider.class);
    private static File rtidbDir() {
        File directory = new File(".");
        directory = directory.getAbsoluteFile();
        while (null != directory) {
            if (directory.isDirectory() && "rtidb".equals(directory.getName())) {
                break;
            }
            logger.debug("current directory name {}", directory.getName());
            directory = directory.getParentFile();
        }

        if ("rtidb".equals(directory.getName())) {
            return directory;
        } else {
            return null;
        }
    }

    public static FesqlDataProvider dataProviderGenerator(String caseFile)
            throws FileNotFoundException {
        Yaml yaml = new Yaml();
        String rtidbDir = rtidbDir().getAbsolutePath();
        Assert.assertNotNull(rtidbDir);
        String caseAbsPath = rtidbDir + "/fesql/cases/" + caseFile;
        logger.debug("fesql case absolute path: {}", caseAbsPath);
        FileInputStream testDataStream = new FileInputStream(caseAbsPath);
        FesqlDataProvider testDateProvider = yaml.loadAs(testDataStream, FesqlDataProvider.class);
        return testDateProvider;
    }


}
