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
package com._4paradigm.openmldb.test_common.restful.model;


import com._4paradigm.openmldb.test_common.util.Tool;
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
public class RestfulCaseFile {
    private List<String> debugs;
    private RestfulCase baseCase;
    private List<RestfulCase> cases;

//    public static final Pattern PATTERN = Pattern.compile("<(.*?)>");

    public List<RestfulCase> getCases(List<Integer> levels) {
        if (!CollectionUtils.isEmpty(debugs)) {
            return getCases();
        }
        List<RestfulCase> cases = getCases().stream()
                .filter(sc -> levels.contains(sc.getLevel()))
                .collect(Collectors.toList());
        return cases;
    }

    public List<RestfulCase> getCases() {
        if (CollectionUtils.isEmpty(cases)) {
            return Collections.emptyList();
        }
        List<RestfulCase> testCaseList = new ArrayList<>();
        List<String> debugs = getDebugs();
        for (RestfulCase tmpCase : cases) {
            if(baseCase!=null){
                Tool.mergeObject(baseCase,tmpCase);
            }
            if (!CollectionUtils.isEmpty(debugs)) {
                if (debugs.contains(tmpCase.getDesc().trim())) {
                    addCase(tmpCase, testCaseList);
                }
                continue;
            }
            if (isCaseInBlackList(tmpCase)) {
                continue;
            }
            addCase(tmpCase, testCaseList);
        }
        return testCaseList;
    }

    private boolean isCaseInBlackList(RestfulCase tmpCase) {
        if (tmpCase == null) return false;
        List<String> tags = tmpCase.getTags();
        if (tags != null && (tags.contains("TODO") || tags.contains("todo"))) {
            return true;
        }
        return false;
    }

    private void addCase(RestfulCase tmpCase, List<RestfulCase> testCaseList) {
        Map<String, List<String>> uriParameters = tmpCase.getUriParameters();
        Map<String, List<String>> bodyParameters = tmpCase.getBodyParameters();
        if (MapUtils.isEmpty(uriParameters) && MapUtils.isEmpty(bodyParameters)) {
            testCaseList.add(tmpCase);
            return;
        }
        if (MapUtils.isNotEmpty(uriParameters) && MapUtils.isNotEmpty(bodyParameters)) {
            List<String> uris = new ArrayList<>();
            Tool.genStr(tmpCase.getUri(), uriParameters, uris);
            List<String> bodies = new ArrayList<>();
            Tool.genStr(tmpCase.getBody(), bodyParameters, bodies);
            for (int i = 0; i < uris.size(); i++) {
                String newUri = uris.get(i);
                for (int j = 0; j < bodies.size(); j++) {
                    RestfulCase newCase = SerializationUtils.clone(tmpCase);
                    String newBody = bodies.get(j);
                    newCase.setUri(newUri);
                    newCase.setBody(newBody);
                    newCase.setCaseId(newCase.getCaseId() + "_" + i+ "_" + j);
                    newCase.setDesc(newCase.getDesc() + "_" + i+ "_" + j);
                    testCaseList.add(newCase);
                }
            }
            return;
        }
        if (MapUtils.isNotEmpty(uriParameters)) {
            testCaseList.addAll(genUriCase(tmpCase, uriParameters));
        }
        if (MapUtils.isNotEmpty(bodyParameters)) {
            testCaseList.addAll(genBodyCase(tmpCase, bodyParameters));
        }
    }

    private List<RestfulCase> genUriCase(RestfulCase tmpCase, Map<String, List<String>> uriParameters) {
        List<String> uris = new ArrayList<>();
        List<RestfulCase> restfulCaseList = new ArrayList<>();
        Tool.genStr(tmpCase.getUri(), uriParameters, uris);
        for (int i = 0; i < uris.size(); i++) {
            String newUri = uris.get(i);
            RestfulCase newCase = SerializationUtils.clone(tmpCase);
            newCase.setUri(newUri);
            newCase.setCaseId(newCase.getCaseId() + "_" + i);
            newCase.setDesc(newCase.getDesc() + "_" + i);
            if (uriParameters.size() == 1 && CollectionUtils.isNotEmpty(tmpCase.getUriExpect())) {
                Expect newExpect = tmpCase.getUriExpect().get(i);
                setNewExpect(newCase, newExpect);
            }
            restfulCaseList.add(newCase);
        }
        return restfulCaseList;
    }

    private List<RestfulCase> genBodyCase(RestfulCase tmpCase, Map<String, List<String>> bodyParameters) {
        List<String> bodies = new ArrayList<>();
        List<RestfulCase> restfulCaseList = new ArrayList<>();
        Tool.genStr(tmpCase.getBody(), bodyParameters, bodies);
        for (int i = 0; i < bodies.size(); i++) {
            String newBody = bodies.get(i);
            RestfulCase newCase = SerializationUtils.clone(tmpCase);
            newCase.setBody(newBody);
            newCase.setCaseId(newCase.getCaseId() + "_" + i);
            newCase.setDesc(newCase.getDesc() + "_" + i);
            if (bodyParameters.size() == 1 && CollectionUtils.isNotEmpty(tmpCase.getBodyExpect())) {
                Expect newExpect = tmpCase.getBodyExpect().get(i);
                setNewExpect(newCase, newExpect);
            }
            restfulCaseList.add(newCase);
        }
        return restfulCaseList;
    }

    private void setNewExpect(RestfulCase newCase, Expect newExpect) {
        String newCode = newExpect.getCode();
        if (StringUtils.isNotEmpty(newCode)) {
            newCase.getExpect().setCode(newCode);
        }
        String newMsg = newExpect.getMsg();
        if (StringUtils.isNotEmpty(newMsg)) {
            newCase.getExpect().setMsg(newMsg);
        }
        Map<String, Object> newData = newExpect.getData();
        if (MapUtils.isNotEmpty(newData)) {
            newCase.getExpect().setData(newData);
        }
        List<String> newColumns = newExpect.getColumns();
        if (CollectionUtils.isNotEmpty(newColumns)) {
            newCase.getExpect().setColumns(newColumns);
        }
        List<List<Object>> newRows = newExpect.getRows();
        if (CollectionUtils.isNotEmpty(newRows)) {
            newCase.getExpect().setRows(newRows);
        }
        List<String> sqls = newExpect.getSqls();
        if (CollectionUtils.isNotEmpty(sqls)) {
            newCase.getExpect().setSqls(sqls);
        }
    }
}
