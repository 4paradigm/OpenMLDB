package com._4paradigm.openmldb.http_test.executor;


import com._4paradigm.openmldb.test_common.restful.common.OpenMLDBHttp;
import com._4paradigm.openmldb.test_common.restful.model.HttpMethod;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.common.BaseExecutor;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandFactory;
import com._4paradigm.openmldb.http_test.util.HttpUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.testng.Assert;
import org.apache.commons.collections4.CollectionUtils;
import com._4paradigm.openmldb.test_common.command.OpenMLDBCommandUtil;
import com._4paradigm.openmldb.test_common.provider.YamlUtil;
import com._4paradigm.openmldb.test_common.util.Tool;
import com._4paradigm.qa.openmldb_deploy.bean.OpenMLDBInfo;
import com._4paradigm.openmldb.test_common.restful.model.HttpData;
import com.google.gson.Gson;


public class RestfulOnlineExecutor extends BaseExecutor  {

    public RestfulOnlineExecutor(SQLCase sqlCase) {
        super(sqlCase);
    }
   
    protected List<InputDesc> tables = sqlCase.getInputs();
    protected HttpResult httpresult;
    protected String deploy;
    protected OpenMLDBInfo openMLDBInfo= YamlUtil.getObject(Tool.openMLDBDir().getAbsolutePath()+"/out/openmldb_info.yaml",OpenMLDBInfo.class);
    String dbName = "test_apiserver";
    String apiServerUrl;
    OpenMLDBHttp openMLDBHttp = new OpenMLDBHttp();

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
        if(null != sqlCase.getMode() && sqlCase.getMode().contains("procedure-unsupport")){
            return false;
        }        

        return true;
    }

    @Override
    public void prepare() {
        apiServerUrl = "http://"+openMLDBInfo.getApiServerEndpoints().get(0);
        List<InputDesc> tables = sqlCase.getInputs();
        OpenMLDBCommandUtil.createDatabases(openMLDBInfo,dbName,tables);
        OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "set @@global.execute_mode='online'");
        if (!CollectionUtils.isEmpty(tables)) {
            OpenMLDBCommandUtil.createTables(openMLDBInfo, dbName, tables);       
            for (InputDesc table : tables) {
                tableNames.add(table.getName());
            }
        }
        Random r = new Random(System.currentTimeMillis());
        deploy = tableNames.get(0)+String.valueOf(r.nextInt(1000000));
        String tmpSql = sqlCase.getSql().toString().replaceAll("\'","\"");
        String options = sqlCase.getLongWindow()!=null? "OPTIONS(long_windows=\""+sqlCase.getLongWindow()+"\",RANGE_BIAS=\"inf\", ROWS_BIAS=\"inf\") "
                                                        :"OPTIONS(RANGE_BIAS=\"inf\", ROWS_BIAS=\"inf\") ";
        tmpSql = options + tmpSql;                 
        OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "deploy "+deploy+" "+tmpSql);
        String uri = "/dbs/"+dbName+"/deployments/"+deploy;
        HttpResult result = openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,"",HttpMethod.GET);
        if(result.getData().contains("\"code\":-1")){
            openMLDBResult.setMsg("deploy fail");
            return;
        }

        // first table and data set as request, skipped in prepare stage, other tables and data set as base, added
        if (tables.size()>1){
            for (int i=1;i<tables.size();i++){
                for (int j=0;j<tables.get(i).getRows().size();j++){
                    String body = HttpUtil.formatInputs("value",tables.get(i),j,false);
                    uri = "/dbs/"+dbName+"/tables/"+tableNames.get(i);
                    OpenMLDBHttp openMLDBHttp = new OpenMLDBHttp();
                    openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,body,HttpMethod.PUT);
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(){  
        if(openMLDBResult.getMsg().equals("deploy fail")){
            openMLDBResult.setOk(false);
            return;
        }
        List<List<Object>> tmpResults = new ArrayList<List<Object>>();
        HttpResult result = new HttpResult();

        // set row i as request line, insert row i, repeat
        for (int i=0;i<tables.get(0).getRows().size();i++){
            String body = HttpUtil.formatInputs("input",tables.get(0),i,true);
            String uri = "/dbs/"+dbName+"/deployments/"+deploy;
            result = openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,body,HttpMethod.POST);
            Gson gson = new Gson();
            HttpData data = gson.fromJson(result.getData(), HttpData.class);
            if (data.getCode().equals(0)){
                List<Object> tmpResult = (List<Object>)data.getData().get("data").get(0);
                tmpResults.add(tmpResult);
            } else {break;}
            body = HttpUtil.formatInputs("value",tables.get(0),i,false);
            uri = "/dbs/"+dbName+"/tables/"+tableNames.get(0);
            openMLDBHttp.restfulJsonRequest(apiServerUrl,uri,body,HttpMethod.PUT);
        }

        openMLDBResult = HttpUtil.convertHttpResult(sqlCase, result,tmpResults);
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
                String errorMessage = String.format("key %s mismatch in case %s", k,sqlCase.getDesc().toString());
                if (v==null){
                    Assert.assertNull(realColumn.get(k), errorMessage);
                } else if (v instanceof Float ){
                    Assert.assertEquals((float)realColumn.get(k),(float)v,1e-4,errorMessage);
                } else if (v instanceof Double){
                    Assert.assertEquals((double)realColumn.get(k),(double)v,1e-4,errorMessage);
                }  else {
                    Assert.assertEquals(realColumn.get(k),v,errorMessage);
                }
            });
        }
    }

    @Override
    public void tearDown(){
        OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, dbName, "drop deployment "+deploy);
        for(InputDesc table:sqlCase.getInputs()){
            String curDb = table.getDb().length()>0? table.getDb(): dbName;
            OpenMLDBCommandFactory.runNoInteractive(openMLDBInfo, curDb, "drop table "+table.getName());
        }
    }

}
