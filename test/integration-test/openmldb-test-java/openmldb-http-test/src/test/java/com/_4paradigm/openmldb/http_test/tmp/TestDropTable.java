package com._4paradigm.openmldb.http_test.tmp;

import com._4paradigm.openmldb.java_sdk_test.common.FedbClient;
import com._4paradigm.openmldb.java_sdk_test.entity.FesqlResult;
import com._4paradigm.openmldb.java_sdk_test.util.FesqlUtil;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com._4paradigm.openmldb.test_common.restful.util.HttpRequest;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.testng.annotations.Test;

import java.util.HashMap;

public class TestDropTable {

    @Test
    public void testAll() throws Exception {
        FedbClient fedbClient = new FedbClient("172.24.4.55:10000","/fedb");
        String apiserver = "172.24.4.55:20000";
        String dbName = "test_zw";
        String url = String.format("http://%s/dbs/%s/tables",apiserver,dbName);
        HttpResult httpResult = HttpRequest.get(url, null, new HashMap<>());
//        System.out.println(httpResult.getData());
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonObject jsonObject = parser.parse(httpResult.getData()).getAsJsonObject();
        JsonArray tables = jsonObject.getAsJsonArray("tables");
        for(int i=0;i<tables.size();i++){
            String name = tables.get(i).getAsJsonObject().get("name").getAsString();
            String sql = "drop table "+name+";";
            FesqlResult ddl = FesqlUtil.ddl(fedbClient.getExecutor(), dbName, sql);
        }
        
    }
}
