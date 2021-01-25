package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.common.FesqlConfig;
import com._4paradigm.fesql_auto_test.util.Tool;
import org.testng.Assert;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class FesqlDataProviderList {
    private List<FesqlDataProvider> dataProviderList = new ArrayList<FesqlDataProvider>();

    public List<SQLCase> getCases() {
        List<SQLCase> cases = new ArrayList<SQLCase>();
        for (FesqlDataProvider dataProvider : dataProviderList) {
            cases.addAll(dataProvider.getCases(FesqlConfig.levels));
        }
        return cases;
    }

    public static FesqlDataProviderList dataProviderGenerator(String[] caseFiles) throws FileNotFoundException {
        FesqlDataProviderList fesqlDataProviderList = new FesqlDataProviderList();
        for (String caseFile : caseFiles) {
            String casePath = Tool.getCasePath(caseFile);
            File file = new File(casePath);
            if(!file.exists()){
                continue;
            }
            if(file.isFile()) {
                fesqlDataProviderList.dataProviderList.add(FesqlDataProvider.dataProviderGenerator(casePath));
            }else{
                File[] files = file.listFiles(f->f.getName().endsWith(".yaml"));
                for(File f:files){
                    fesqlDataProviderList.dataProviderList.add(FesqlDataProvider.dataProviderGenerator(f.getAbsolutePath()));
                }
            }
        }
        return fesqlDataProviderList;
    }

}