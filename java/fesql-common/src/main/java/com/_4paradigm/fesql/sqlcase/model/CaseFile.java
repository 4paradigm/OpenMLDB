package com._4paradigm.fesql.sqlcase.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;

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
            if (!isCaseInBlackList(tmpCase)) {
                testCaseList.add(tmpCase);
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

}
