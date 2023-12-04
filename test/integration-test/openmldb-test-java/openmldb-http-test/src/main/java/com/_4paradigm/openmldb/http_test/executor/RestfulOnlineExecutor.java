package com._4paradigm.openmldb.http_test.executor;


import com._4paradigm.openmldb.test_common.restful.common.OpenMLDBHttp;
import com._4paradigm.openmldb.test_common.restful.model.HttpMethod;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.SQLCaseType;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.common.BaseExecutor;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFactory;
import com._4paradigm.openmldb.http_test.util.HttpUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.apache.commons.collections4.CollectionUtils;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandUtil;
import com._4paradigm.openmldb.test_common.provider.YamlUtil;
import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.openmldb.http_test.common.ClusterTest;
public class RestfulOnlineExecutor extends BaseExecutor  {

    public RestfulOnlineExecutor(SQLCase sqlCase) {
        super(sqlCase);
    }
   
    protected List<InputDesc> tables = sqlCase.getInputs();
    protected HttpResult httpresult;
    protected String deploy;
    String apiServerUrl = "http://node-3:20546/";
    String dbName = "dbd";
    protected OpenMLDBInfo openMLDBInfo= YamlUtil.getObject(Tool.openMLDBDir().getAbsolutePath()+"/out/openmldb_info.yaml",OpenMLDBInfo.class);

    @Override
    public boolean verify(){
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("request-unsupport")){
            return false;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("cluster-unsupport")){
            return false;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("hybridse-only")){
            return false;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("apiserver-unsupport")){
            return false;
        }
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("rtidb-unsupport")){
            return false;
        }
        
        

        return true;
    }



    @Override
    public void prepare() {
        List<InputDesc> tables = sqlCase.getInputs();
        SQLCase a = sqlCase;
        for (int i = 0; i < tables.size(); i++ ) {
            String curDb = tables.get(i).getDb().length()>0? tables.get(i).getDb(): dbName;
            if (!CollectionUtils.isEmpty(tables)) {
                OpenMLDBCommandUtil.createTables(openMLDBInfo, curDb, tables);       
                for (InputDesc table : tables) {
                    tableNames.add(table.getName());
                }
            }
        }
        Random r = new Random(System.currentTimeMillis());
        deploy = tableNames.get(0)+String.valueOf(r.nextInt(1000000));
        String tmpSql = sqlCase.getSql();
        if (sqlCase.getLongWindow()!=null) {
            tmpSql = String.format("OPTIONS(long_windows=\"%s\") ", sqlCase.getLongWindow())+tmpSql;
        } else {
           // tmpSql ="OPTIONS(RANGE_BIAS='inf', ROWS_BIAS='inf') "+tmpSql;
        }
        OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "deploy "+deploy+" "+tmpSql);

        // first table and data set as request, skipped in prepare stage, other tables and data set as base, added
        if (tables.size()>1){
            for (int i=1;i<tables.size();i++){
                for (int j=0;j<tables.get(i).getRows().size();j++){
                    String body = HttpUtil.formatInputs("value",tables.get(i),j,false);
                    String uri = "dbs/"+dbName+"/tables/"+tableNames.get(i);
                    OpenMLDBHttp openMLDBHttp = new OpenMLDBHttp();
                    openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,body,HttpMethod.PUT);
                }
            }
        }
    }

    @Override
    public void execute(){
        String deploymentName = deploy;
        
        OpenMLDBHttp openMLDBHttp = new OpenMLDBHttp();
        
        List<List<Object>> tmpResults = new ArrayList<List<Object>>();
        boolean success = true;
        HttpResult result = new HttpResult();

        // set row i as request line, insert row i, repeat
        for (int i=0;i<tables.get(0).getRows().size();i++){
            String body = HttpUtil.formatInputs("input",tables.get(0),i,true);
            String uri = "dbs/"+dbName+"/deployments/"+deploymentName;
            result = openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,body,HttpMethod.POST);
            if (result.getData().getCode().equals(0)){
                List<Object> tmpResult = (List<Object>)result.getData().getData().get("data").get(0);
                tmpResults.add(tmpResult);
            } else {
                break;
            }
            body = HttpUtil.formatInputs("value",tables.get(0),i,false);
            uri = "dbs/"+dbName+"/tables/"+tableNames.get(0);
            openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,body,HttpMethod.PUT);
        }


        openMLDBResult = HttpUtil.convertHttpResult(sqlCase, result,tmpResults);
       // return openMLDBResult;
    }

    @Override
    public void check(){
        // success check 
        if (!sqlCase.getExpect().getSuccess()){
            Assert.assertFalse(openMLDBResult.isOk(),"execute expect fail but success");
            return;
       }
       // format output
       openMLDBResult.setFormattedExpectResults(HttpUtil.FormatOutputs(sqlCase.getExpect().getRows(),sqlCase.getExpect().getColumns()));
       openMLDBResult.setFormattedActualResults(HttpUtil.FormatOutputs(openMLDBResult.getResult(),openMLDBResult.getColumnTypes()));

        // size check
        //Assert.assertEquals(openMLDBResult.isOk(),sqlCase.getExpect().getSuccess().booleanValue(),"errror  "+openMLDBResult.getMsg() );
        Assert.assertEquals(openMLDBResult.getFormattedActualResults().size(),openMLDBResult.getFormattedExpectResults().size());

        // contents check
        for (int i =0;i<openMLDBResult.getFormattedActualResults().size();i++){
            Map<String,Object> realColumn = openMLDBResult.getFormattedActualResults().get(i);
            Map<String,Object> expectColumn = openMLDBResult.getFormattedExpectResults().get(i);
            expectColumn.forEach((k,v)-> {
                Assert.assertTrue(realColumn.containsKey(k), "column "+k+"don't exist");
                if (expectColumn.get(k)==null){
                    Assert.assertNull(realColumn.get(k), k+" is not null");
                } else if (expectColumn.get(k) instanceof Float ){
                    Assert.assertEquals((float)realColumn.get(k),(float)expectColumn.get(k),1e-4,
                        "error key value is "+ k);
                } else if (expectColumn.get(k) instanceof Double){
                    Assert.assertEquals((double)realColumn.get(k),(double)expectColumn.get(k),1e-4,
                        "error key value is "+ k);
                } 
                 else {
                    Assert.assertEquals(realColumn.get(k),expectColumn.get(k),
                        "error key value is "+ k);
                }
            });

        }
    }

    @Override
    public void tearDown(){
        String tableName;
        tableName =tableNames.get(0);
        OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "drop deployment "+deploy);
        for(InputDesc table:sqlCase.getInputs()){
            String curDb = table.getDb().length()>0? table.getDb(): dbName;
            OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, curDb, "drop table "+table.getName());
        }
        // for (int i = 0; i < tables.size(); i++ ) {
        //     if(tables.get(i).getDb().length()>0 ){
        //         //dbName = tables.get(i).getDb();
        //         tableName =tableNames.get(0);
        //         OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "drop deployment "+deploy);
        //         OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "drop table "+tableName);
        //     }
        // }
    }



}
