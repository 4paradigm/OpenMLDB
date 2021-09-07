/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.test_common.model;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class CaseFile {
    private String db;
    private List<String> debugs;
    private List<SQLCase> cases;
    // ANSISQL  HybridSQL  SQLITE3 MYSQL
    private List<String> sqlDialect = Lists.newArrayList("ANSISQL");

    public List<SQLCase> getCases(List<Integer> levels) {
        if(!CollectionUtils.isEmpty(debugs)){
            return getCases();
        }
        List<SQLCase> cases = getCases().stream()
                .filter(sc -> levels.contains(sc.getLevel()))
                .collect(Collectors.toList());
        return cases;
    }

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
            if(CollectionUtils.isEmpty(tmpCase.getSqlDialect())){
                tmpCase.setSqlDialect(sqlDialect);
            }
            if (!CollectionUtils.isEmpty(debugs)) {
                if (debugs.contains(tmpCase.getDesc().trim())) {
                    addCase(tmpCase,testCaseList);
                }
                continue;
            }
            if (isCaseInBlackList(tmpCase)) {
                continue;
            }
            addCase(tmpCase,testCaseList);
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
    private void addCase(SQLCase tmpCase, List<SQLCase> testCaseList){
        List<List<String>> dataProviderList = tmpCase.getDataProvider();
        if(CollectionUtils.isNotEmpty(dataProviderList)){
            List<SQLCase> genList = generateCase(0,tmpCase,dataProviderList);
            testCaseList.addAll(genList);
        }else {
            testCaseList.add(tmpCase);
        }
    }

    private List<SQLCase> generateCase(int index, SQLCase sqlCase, List<List<String>> dataProviderList){
        List<String> dataProvider = dataProviderList.get(index);
        List<SQLCase> caseList = generateCaseByDataProvider(sqlCase, dataProvider,index);
        if(dataProviderList.size()-1==index){
            return caseList;
        }
        List<SQLCase> sqlCases = new ArrayList<>();
        for(SQLCase tmpCase:caseList){
            // generateCase(index+1,tmpCase,dataProviderList);
            List<SQLCase> genCaseList = generateCase(index+1,tmpCase,dataProviderList);
            sqlCases.addAll(genCaseList);
        }
        return sqlCases;
    }

    private List<SQLCase> generateCaseByDataProviderList(SQLCase sqlCase, List<List<String>> dataProviderList){
        if(dataProviderList.size()==1){
            List<String> dataProvider = dataProviderList.get(0);
            List<SQLCase> caseList = generateCaseByDataProvider(sqlCase, dataProvider,0);
            return caseList;
        }else if(dataProviderList.size()==2){
            List<SQLCase> sqlCases = new ArrayList<>();
            List<String> dataProvider0 = dataProviderList.get(0);
            List<SQLCase> caseList0 = generateCaseByDataProvider(sqlCase, dataProvider0,0);
            List<String> dataProvider1 = dataProviderList.get(1);
            for(SQLCase tmpCase:caseList0){
                List<SQLCase> caseList1 = generateCaseByDataProvider(tmpCase,dataProvider1,1);
                sqlCases.addAll(caseList1);
            }
            return sqlCases;
        } else if(dataProviderList.size()==3){
            List<String> dataProvider0 = dataProviderList.get(0);
            List<SQLCase> caseList0 = generateCaseByDataProvider(sqlCase, dataProvider0,0);
            List<String> dataProvider1 = dataProviderList.get(1);
            List<SQLCase> sqlCases1 = new ArrayList<>();
            for(SQLCase tmpCase:caseList0){
                List<SQLCase> caseList1 = generateCaseByDataProvider(tmpCase,dataProvider1,1);
                sqlCases1.addAll(caseList1);
            }
            List<String> dataProvider2 = dataProviderList.get(2);
            List<SQLCase> sqlCases2 = new ArrayList<>();
            for(SQLCase tmpCase:sqlCases1){
                List<SQLCase> caseList2 = generateCaseByDataProvider(tmpCase,dataProvider2,2);
                sqlCases2.addAll(caseList2);
            }
            return sqlCases2;
        }
        return null;
    }

    private List<SQLCase> generateCaseByDataProvider(SQLCase sqlCase, List<String> dataProvider, int index){
        List<SQLCase> sqlCases = new ArrayList<>();
        for(int i=0;i<dataProvider.size();i++){
            String data = dataProvider.get(i);
            String sql = sqlCase.getSql();
            sql = sql.replaceAll("d\\["+index+"\\]",data);
            SQLCase newSqlCase = SerializationUtils.clone(sqlCase);
            //设置新的sql
            newSqlCase.setSql(sql);
            newSqlCase.setId(newSqlCase.getId()+"_"+i);
            newSqlCase.setDesc(newSqlCase.getDesc()+"_"+i);
            //根据expectProvider 生成新的 预期结果 只对第一级测dataProvider可以设置不同的expect
            if(index==0) {
                Map<Integer, ExpectDesc> map = sqlCase.getExpectProvider();
                if (MapUtils.isNotEmpty(map)) {
                    ExpectDesc expectDesc = map.get(i);
                    if (expectDesc != null) {
                        ExpectDesc newExpectDesc = newSqlCase.getExpect();
                        if(newExpectDesc==null) {
                            newSqlCase.setExpect(expectDesc);
                        }else {
                            boolean success = expectDesc.getSuccess();
                            String order = expectDesc.getOrder();
                            List<String> columns = expectDesc.getColumns();
                            List<List<Object>> rows = expectDesc.getRows();
                            int count = expectDesc.getCount();
                            if (success == false) newExpectDesc.setSuccess(success);
                            if (count > 0) newExpectDesc.setCount(count);
                            if (CollectionUtils.isNotEmpty(columns)) newExpectDesc.setColumns(columns);
                            if (StringUtils.isNotEmpty(order)) newExpectDesc.setOrder(order);
                            if (CollectionUtils.isNotEmpty(rows)) newExpectDesc.setRows(rows);
                        }
                    }
                }
            }
            sqlCases.add(newSqlCase);
        }
        return sqlCases;
    }

}
