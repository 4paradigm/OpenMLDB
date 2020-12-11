package com._4paradigm.fesql.sqlcase.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.SerializationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
@Setter
@ToString
public class CaseFile {
    String db;
    List<String> debugs;
    List<SQLCase> cases;

    public List<SQLCase> getCases() {
        if (CollectionUtils.isEmpty(cases)) {
            return Collections.emptyList();
        }
        List<SQLCase> testCaseList = new ArrayList<>();
        List<String> debugs = getDebugs();
        for (SQLCase tmpCase : cases) {
            if (null == tmpCase.getDb()) {
                tmpCase.setDb(getDb());
            }
            if (!CollectionUtils.isEmpty(debugs)) {
                if (debugs.contains(tmpCase.getDesc().trim())) {
                    testCaseList.add(tmpCase);
                }
                continue;
            }
            if (isCaseInBlackList(tmpCase)) {
                continue;
            }
            if(CollectionUtils.isNotEmpty(tmpCase.getDataProvider())){
                List<SQLCase> genList = generateCaseByDataProvider(tmpCase,tmpCase.getDataProvider());
                cases.addAll(genList);
            }else {
                cases.add(tmpCase);
            }
        }
        return testCaseList;
    }

    private boolean isCaseInBlackList(SQLCase tmpCase) {
        if (tmpCase == null) return false;
        List<String> tags = tmpCase.getTags();
        if (tags != null && (tags.contains("TODO") || tags.contains("todo"))) {
            return true;
        }
        return false;
    }
    private List<SQLCase> generateCaseByDataProvider(SQLCase sqlCase,List<String> dataProvider){
        List<SQLCase> sqlCases = new ArrayList<>();
        for(String data:dataProvider){
            String sql = sqlCase.getSql();
            sql = sql.replaceAll("\\[0\\]",data);
            SQLCase newSqlCase = SerializationUtils.clone(sqlCase);
            newSqlCase.setSql(sql);
            sqlCases.add(newSqlCase);
        }
        return sqlCases;
    }

}
