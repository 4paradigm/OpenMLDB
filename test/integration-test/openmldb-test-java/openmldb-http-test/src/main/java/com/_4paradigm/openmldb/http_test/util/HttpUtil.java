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
package com._4paradigm.openmldb.http_test.util;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com._4paradigm.openmldb.test_common.model.InputDesc;
import com._4paradigm.openmldb.test_common.bean.OpenMLDBResult;
import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import lombok.extern.slf4j.Slf4j;
import com._4paradigm.openmldb.test_common.restful.model.HttpData;

@Slf4j
public class HttpUtil {
    public static String formatInputs(String key, InputDesc table,int cur, boolean need_schema){
        String body = "";
        Gson gson = new GsonBuilder().create();

        List<List<Object>> rows = table.getRows();
        List<List<Object>> tmpRow = new ArrayList<List<Object>>();
        Map<String, Object> data = new HashMap<>();
        tmpRow.add(rows.get(cur));
        // for(List<Object> row : rows) {
        //     tmpRow.add(row);
        // }
        data.put(key, tmpRow);
        if (need_schema){
            data.put("need_schema", true);
        }
        body = gson.toJson(data);

        return body;
    }


    public static List<Map<String, Object>> FormatOutputs(List<List<Object>> rows,List<String> columns){
        String[] nameType;
        Object o;
        List<Map<String, Object>> resultLists = new ArrayList<Map<String, Object>>();
        List<Object> resultList = new ArrayList<Object>();
        if (rows.equals(null)||rows==null||rows.size()==0){return resultLists;}
        Collections.sort(rows, new RowsSort(0));
        for (int i=0;i<rows.size();i++) {
            resultList = rows.get(i);
            Map<String, Object> tmp = new HashMap<>();
            for (int j=0;j<resultList.size();j++){
                String name = "";
                String type = "";
                nameType = columns.get(j).split(" ");
                for (int k=0; k<nameType.length;k++){
                    if (k == nameType.length-1) {type = switchType(nameType[k]);break;}
                    name = name + nameType[k];
                }
                o = resultList.get(j);
                if (o==null){
                    tmp.put(name, null);
                    continue;
                }
                Object data = new Object();
                switch (type) {
                    case "int":
                        o = Double.parseDouble(o.toString());
                        data = (new Double((double)o)).intValue(); break;
                    case "double":
                        data = Double.parseDouble(o.toString());break;               
                    case "bool":
                        data = (Boolean)o;break;
                    case "string":
                        data = (String)o;break;
                    case "long":
                        o = Double.parseDouble(o.toString());
                        data = (new Double((double)o)).longValue(); break;
                    case "float":
                        o = Double.parseDouble(o.toString());
                        data = (new Double((double)o)).floatValue(); break;        
                    case "timestamp":
                        o = Double.parseDouble(o.toString());
                        long ts = (new Double((double)o)).longValue();
                        data = new java.sql.Timestamp(ts);break;
                    case "date":
                       DateFormat fmt =new SimpleDateFormat("yyyy-MM-dd");
                       try {
                        data = fmt.parse(o.toString()).getTime();
                       } catch (Exception e) {
                        log.error("parse date fail");
                       }
                        break;
                    default:
                        log.error("type unkown "+type.toString());
                        data = (String)o;
                }
                tmp.put(name, data);
            }
            resultLists.add(i, tmp);
        }
        return resultLists;   
    }

    @SuppressWarnings("unchecked")
    public static OpenMLDBResult convertHttpResult(SQLCase sqlCase, HttpResult httpResult){
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        Gson gson = new Gson();
        HttpData data = gson.fromJson(httpResult.getData(), HttpData.class);
        openMLDBResult.setMsg(sqlCase.getDesc()+data.getMsg());
        if (httpResult.getHttpCode()==200 && data.getCode()==0 ){
            openMLDBResult.setOk(true);
            try {
                List<Object> httpData =  data.getData().get("data");
                List<List<Object>> resultList = new ArrayList<List<Object>>();
                for (Object o : httpData) {
                    resultList.add((List<Object>)o);
                }
                if (resultList.size()>0) {
                    openMLDBResult.setResult(resultList);
                }
                
                httpData = data.getData().get("schema");
                List<String> schemaList = new ArrayList<String>();
                for (int i = 0 ; i<httpData.size();i++) {
                    Map<String,String> mp = (Map<String,String>)httpData.get(i);
                    schemaList.add(mp.get("name")+" "+mp.get("type"));
                }
                openMLDBResult.setColumnTypes(schemaList);
            } catch (Exception e) {
               //
            }
        } else {
            openMLDBResult.setOk(false);
          //  Assert.assertNotEquals(0, httpResult.getData().getCode());
        }
        return openMLDBResult;
    }

    @SuppressWarnings("unchecked")
    public static OpenMLDBResult convertHttpResult(SQLCase sqlCase, HttpResult httpResult, List<List<Object>> tmpResults){
        OpenMLDBResult openMLDBResult = new OpenMLDBResult();
        Gson gson = new Gson();
        HttpData data = gson.fromJson(httpResult.getData(), HttpData.class);
        openMLDBResult.setMsg(sqlCase.getDesc()+data.getMsg());
        if (httpResult.getHttpCode()!=200 || data.getCode()!=0 ){
            openMLDBResult.setOk(false);
            return openMLDBResult;
        }
        try {
            List<Object> httpData =  data.getData().get("data");
            openMLDBResult.setResult(tmpResults);               
            httpData = data.getData().get("schema");
            List<String> schemaList = new ArrayList<String>();
            for (int i = 0 ; i<httpData.size();i++) {
                Map<String,String> mp = (Map<String,String>)httpData.get(i);
                schemaList.add(mp.get("name").replace(" ", "")+" "+mp.get("type"));
            }
            
            openMLDBResult.setColumnTypes(schemaList);
            openMLDBResult.setOk(true);
        } catch (Exception e) {
            e.printStackTrace();
            log.info( "erro msg is "+data.getMsg());
            openMLDBResult.setOk(false);
        }
        return openMLDBResult;
    }

    public static String switchType(String type){
        switch(type){
            case "smallint":
                return "int";
            case "int":
                return "int";
            case "bigint":
                return "long";
            case "int32":
                return "int";
            case "int16":
                return "int";
            case "int64":
                 return "long";
            case "float":
                return "float";               
            default:
                return type;
        }

    }

}
