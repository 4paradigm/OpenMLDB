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
package com._4paradigm.openmldb.http_test.executor;


import com._4paradigm.openmldb.http_test.check.CheckerStrategy;
import com._4paradigm.openmldb.http_test.common.RestfulGlobalVar;
import com._4paradigm.openmldb.http_test.config.FedbRestfulConfig;
import com._4paradigm.openmldb.java_sdk_test.checker.ResultChecker;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFacade;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandUtil;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.common.Checker;
import com._4paradigm.openmldb.test_common.model.ExpectDesc;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.restful.common.OpenMLDBHttp;
import com._4paradigm.openmldb.test_common.restful.model.AfterAction;
import com._4paradigm.openmldb.test_common.restful.model.BeforeAction;
import com._4paradigm.openmldb.test_common.restful.model.HttpMethod;
import com._4paradigm.openmldb.test_common.restful.model.RestfulCase;
import com._4paradigm.openmldb.test_common.util.SQLUtil;
import com._4paradigm.openmldb.test_common.util.Tool;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.text.ParseException;
import java.util.List;
import java.util.stream.Collectors;

public class RestfulCliExecutor extends BaseExecutor{
    private OpenMLDBHttp fedbHttp;
    public RestfulCliExecutor(RestfulCase restfulCase) {
        super(restfulCase);
        fedbHttp = new OpenMLDBHttp();
        fedbHttp.setUrl("http://"+ RestfulGlobalVar.mainInfo.getApiServerEndpoints().get(0));
        fedbHttp.setMethod(HttpMethod.valueOf(restfulCase.getMethod()));
        String uri = restfulCase.getUri();
        if(uri.contains("{db_name}")){
            uri = uri.replace("{db_name}", FedbRestfulConfig.DB_NAME);
        }
        fedbHttp.setUri(uri);
        fedbHttp.setBody(restfulCase.getBody());
        fedbHttp.setIsJson(true);
        if(MapUtils.isNotEmpty(restfulCase.getHeaders())){
            fedbHttp.setHeadMap(restfulCase.getHeaders());
        }
    }

    @Override
    public void prepare() {
        OpenMLDBResult createDBResult = OpenMLDBCommandUtil.createDB(RestfulGlobalVar.mainInfo, FedbRestfulConfig.DB_NAME);
        logger.info("create db:{},{}", FedbRestfulConfig.DB_NAME, createDBResult.isOk());
        BeforeAction beforeAction = restfulCase.getBeforeAction();
        if(beforeAction==null){
            logger.info("no before action");
            return;
        }
        if(CollectionUtils.isNotEmpty(beforeAction.getTables())) {
            OpenMLDBResult res = OpenMLDBCommandUtil.createAndInsert(RestfulGlobalVar.mainInfo, FedbRestfulConfig.DB_NAME, beforeAction.getTables());
            if (!res.isOk()) {
                throw new RuntimeException("fail to run BatchSQLExecutor: prepare fail ");
            }
            String uri = fedbHttp.getUri();
            if(uri.contains("{table_name}")){
                uri = uri.replace("{table_name}",beforeAction.getTables().get(0).getName());
            }
            fedbHttp.setUri(uri);
            tableNames = beforeAction.getTables().stream().map(t->t.getName()).collect(Collectors.toList());
        }
        if(CollectionUtils.isNotEmpty(beforeAction.getSqls())){
            List<String> sqls = beforeAction.getSqls().stream()
                    .map(sql -> SQLUtil.formatSql(sql,tableNames, RestfulGlobalVar.mainInfo))
                    .map(sql->{
                        if(sql.contains("{db_name}")){
                            sql = sql.replace("{db_name}",FedbRestfulConfig.DB_NAME);
                        }
                        return sql;
                    })
                    .collect(Collectors.toList());
            OpenMLDBCommandFacade.sqls(RestfulGlobalVar.mainInfo,FedbRestfulConfig.DB_NAME,sqls);
        }
        logger.info("prepare end");
    }

    @Override
    public void execute() {
        Tool.sleep(3000);
        httpResult = fedbHttp.restfulRequest();
    }

    @Override
    public void check() throws Exception {
        List<Checker> strategyList = CheckerStrategy.build(restfulCase,httpResult);
        for (Checker checker : strategyList) {
            checker.check();
        }
    }

    @Override
    public void tearDown() {
        logger.info("tearDown begin");
        AfterAction tearDown = restfulCase.getTearDown();
        if(tearDown!=null){
            if(CollectionUtils.isNotEmpty(tearDown.getSqls())){
                List<String> sqls = tearDown.getSqls().stream()
                        .map(sql -> SQLUtil.formatSql(sql,tableNames, RestfulGlobalVar.mainInfo))
                        .map(sql->{
                            if(sql.contains("{db_name}")){
                                sql = sql.replace("{db_name}",FedbRestfulConfig.DB_NAME);
                            }
                            return sql;
                        })
                        .collect(Collectors.toList());
                fesqlResult = OpenMLDBCommandFacade.sqls(RestfulGlobalVar.mainInfo, FedbRestfulConfig.DB_NAME, sqls);
            }
        }

        if(restfulCase.getBeforeAction()!=null){
            List<InputDesc> tables = restfulCase.getBeforeAction().getTables();
            if (CollectionUtils.isEmpty(tables)) {
                return;
            }
            for (InputDesc table : tables) {
                if(table.isDrop()) {
                    String drop = "drop table " + table.getName() + ";";
                    OpenMLDBCommandFacade.sql(RestfulGlobalVar.mainInfo, FedbRestfulConfig.DB_NAME, drop);
                }
            }
        }
        logger.info("tearDown end");
    }
    @Override
    protected void afterAction(){
        AfterAction afterAction = restfulCase.getAfterAction();
        if(afterAction!=null){
            if(CollectionUtils.isNotEmpty(afterAction.getSqls())){
                List<String> sqls = afterAction.getSqls().stream()
                        .map(sql -> SQLUtil.formatSql(sql,tableNames, RestfulGlobalVar.mainInfo))
                        .collect(Collectors.toList());
                fesqlResult = OpenMLDBCommandFacade.sqls(RestfulGlobalVar.mainInfo, FedbRestfulConfig.DB_NAME, sqls);
            }
            ExpectDesc expect = afterAction.getExpect();
            if(expect!=null){
                checkAfterAction(expect);
            }
        }
    }
    private void checkAfterAction(ExpectDesc expect){
        try {
            new ResultChecker(expect,fesqlResult).check();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
