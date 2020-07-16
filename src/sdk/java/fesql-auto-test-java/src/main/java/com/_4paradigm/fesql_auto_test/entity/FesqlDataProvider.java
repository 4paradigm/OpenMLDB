package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
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
public class FesqlDataProvider {
    private static Logger logger = LoggerFactory.getLogger(FesqlDataProvider.class);
    private String db;
    private SQLCase[] cases;
    private List<String> debugs;

    public static File rtidbDir() {
        File directory = new File(".");
        directory = directory.getAbsoluteFile();
        while (null != directory) {
            if (directory.isDirectory() && "rtidb".equals(directory.getName())) {
                break;
            }
            logger.info("current directory name {}", directory.getName());
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
        logger.info("fesql case absolute path: {}", caseAbsPath);
        FileInputStream testDataStream = new FileInputStream(caseAbsPath);
        FesqlDataProvider testDateProvider = yaml.loadAs(testDataStream, FesqlDataProvider.class);
        return testDateProvider;
    }

    public SQLCase[] getCases() {
        List<SQLCase> testCaseList = new ArrayList<>();
        for (SQLCase tmpCase : cases) {
            if (null == tmpCase.getDb()) {
                tmpCase.setDb(this.db);
            }
            if (!CollectionUtils.isEmpty(debugs)) {
                if (debugs.contains(tmpCase.getDesc().trim())) {
                    testCaseList.add(tmpCase);
                }
                continue;
            }
            if (!isCaseInBlackList(tmpCase)) {
                testCaseList.add(tmpCase);
            }
        }
        return testCaseList.toArray(new SQLCase[testCaseList.size()]);
    }


    public boolean isCaseInBlackList(SQLCase tmpCase) {
        if (tmpCase == null) return false;
        List<String> tags = tmpCase.getTags();
        if (tags != null && (tags.contains("TODO") || tags.contains("todo"))) {
            return true;
        }
        return false;
    }
}
