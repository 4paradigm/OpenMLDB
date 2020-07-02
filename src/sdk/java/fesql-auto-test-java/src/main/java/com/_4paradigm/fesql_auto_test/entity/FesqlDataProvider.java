package com._4paradigm.fesql_auto_test.entity;

import lombok.Data;
import org.yaml.snakeyaml.Yaml;

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
    private String db;
    private FesqlCase[] cases;
    private List<String> debugs;
    private String executor;

    public static FesqlDataProvider dataProviderGenerator(String caseFile)
            throws FileNotFoundException {
        Yaml yaml = new Yaml();
        FileInputStream testDataStream = new FileInputStream(FesqlDataProvider.class.getResource(caseFile).getPath());
        FesqlDataProvider testDateProvider = yaml.loadAs(testDataStream, FesqlDataProvider.class) ;
        return testDateProvider;
    }
    public FesqlCase[] getCases() {
        if (debugs !=null&& debugs.size() > 0) {
            FesqlCase[] caseList = new FesqlCase[debugs.size()];
            int i = 0;
            for (FesqlCase tmpCase : cases) {
                if (null == tmpCase.getExecutor()) {
                    tmpCase.setExecutor(this.executor);
                }
                if(null == tmpCase.getDb()){
                    tmpCase.setDb(this.db);
                }
                if (debugs.contains(tmpCase.getDesc())) {
                    caseList[i++] = tmpCase;
                }
            }
            return caseList;
        } else {
            List<FesqlCase> testCaseList = new ArrayList<>();
            for (FesqlCase tmpCase : cases) {
                if (null == tmpCase.getExecutor()) {
                    tmpCase.setExecutor(this.executor);
                }
                if(null == tmpCase.getDb()){
                    tmpCase.setDb(this.db);
                }
                if (!isCaseInBlackList(tmpCase)) {
                    testCaseList.add(tmpCase);
                }
            }
            return testCaseList.toArray(new FesqlCase[testCaseList.size()]);
        }
    }


    public boolean isCaseInBlackList(FesqlCase tmpCase) {
        if(tmpCase==null)return false;
        List<String> tag = tmpCase.getTag();
        if(tag!=null&&tag.contains("TODO")){
            return true;
        }
        return false;
    }
}
