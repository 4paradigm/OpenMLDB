package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.SQLCase;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class FesqlDataProviderList {
    private List<FesqlDataProvider> dataProviderList = new ArrayList<FesqlDataProvider>();

    public List<SQLCase> getCases() {
        List<SQLCase> cases = new ArrayList<SQLCase>();
        for (FesqlDataProvider dataProvider : dataProviderList) {
            cases.addAll(dataProvider.getCases());
        }
        return cases;
    }

    public static FesqlDataProviderList dataProviderGenerator(String[] caseFiles) throws FileNotFoundException {
        FesqlDataProviderList fesqlDataProviderList = new FesqlDataProviderList();
        for (String caseFile : caseFiles) {
            fesqlDataProviderList.dataProviderList.add(FesqlDataProvider.dataProviderGenerator(caseFile));
        }
        return fesqlDataProviderList;
    }
}