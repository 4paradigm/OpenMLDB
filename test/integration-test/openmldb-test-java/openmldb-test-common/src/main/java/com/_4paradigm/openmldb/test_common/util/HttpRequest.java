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
package com._4paradigm.openmldb.test_common.util;

import com._4paradigm.openmldb.test_common.restful.model.HttpData;
import com._4paradigm.openmldb.test_common.restful.model.HttpResult;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;
import net.minidev.json.JSONObject;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HttpRequest {
    
    public static boolean debug = false;
    public static HttpResult get(String link) throws Exception {
        return get(link,null,null);
    }
    public static HttpResult get(String link, Map<String, Object> dataMap, Map<String, String> headMap) throws Exception {
        if(dataMap!=null&&dataMap.size()>0){
        	link += "?" + mapToHttpString(dataMap);
        	//log.info("请求的data:" + mapToJson(dataMap));
        }
    	log.info("request url:" + link);
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        HttpGet httpget = new HttpGet(link);
        if(MapUtils.isNotEmpty(headMap)) {
            addHeaders(httpget, headMap);
        }
        debugHeader(httpget.getAllHeaders());
        long beginTime = System.currentTimeMillis();
        CloseableHttpResponse response = httpclient.execute(httpget);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity, "utf-8");
        log.info("response code:" + code + " data:"+ resultString);
        return getHttpResult(resultString, code,headers,cookies,beginTime,endTime);
    }
    public static HttpResult postJson(String link, Map<String, Object> dataMap, Map<String, String> headMap) throws Exception {
    	log.info("request url:" + link + " data: "+mapToJson(dataMap));
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        HttpPost httpost = new HttpPost(link.toString());
        addHeaders(httpost, headMap);
        httpost.setHeader("Content-Type","application/json;charset=utf-8");
        httpost.setEntity(new StringEntity(mapToJson(dataMap), HTTP.UTF_8));
        debugHeader(httpost);
        long beginTime = System.currentTimeMillis();
        HttpResponse response = httpclient.execute(httpost);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity);
        log.info("response code:" + code + " data:"+ resultString);
        return getHttpResult(resultString, code,headers,cookies,beginTime,endTime);
    }
    public static HttpResult postJson(String link, String json, Map<String, String> headMap) throws Exception {
    	log.info("request url:" + link + " data: "+json);
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        HttpPost httpPost = new HttpPost(link.toString());
        addHeaders(httpPost, headMap);
        httpPost.setHeader("Content-Type","application/json;charset=utf-8");
        if(StringUtils.isNotEmpty(json)) {
            httpPost.setEntity(new StringEntity(json, HTTP.UTF_8));
        }
//        httpPost.setEntity(new StringEntity(json, HTTP.UTF_8));
        debugHeader(httpPost);
        long beginTime = System.currentTimeMillis();
        HttpResponse response = httpclient.execute(httpPost);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity);
        log.info("response code:" + code + " data:"+ resultString);
        return getHttpResult(resultString, code,headers,cookies,beginTime,endTime);
    }
    public static HttpResult put(String link, String json, Map<String, String> headMap) throws IOException {
    	log.info("request url:" + link + " data: "+json);
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        HttpPut httpPut = new HttpPut(link.toString());
        if(StringUtils.isNotEmpty(json)) {
            httpPut.setEntity(new StringEntity(json, HTTP.UTF_8));
        }
        addHeaders(httpPut, headMap);
        debugHeader(httpPut);
        long beiginTime = System.currentTimeMillis();
        HttpResponse response = httpclient.execute(httpPut);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity);
        log.info("response code:" + code + " data:"+ resultString);
        return getHttpResult(resultString, code,headers,cookies,beiginTime,endTime);
    }
    public static String uploadFile(String filename, String url) throws IOException {
        log.info("上传文件路径:" + filename);
        log.info("上传文件url:" + url);
        File file = new File(filename);
        String response = "";
        if (!file.exists()) {
            return "file not exists";
        }
        PostMethod postMethod = new PostMethod(url);
        try {
            //FilePart：用来上传文件的类
            FilePart fp = new FilePart("filedata", file);
            Part[] parts = {fp};
            //对于MIME类型的请求，httpclient建议全用MulitPartRequestEntity进行包装
            MultipartRequestEntity mre = new MultipartRequestEntity(parts, postMethod.getParams());
            postMethod.setRequestEntity(mre);
            HttpClient client = new HttpClient();
            client.getHttpConnectionManager().getParams().setConnectionTimeout(50000);// 设置连接时间
            int status = client.executeMethod(postMethod);
            if (status == HttpStatus.SC_OK) {
                InputStream inputStream = postMethod.getResponseBodyAsStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                StringBuffer stringBuffer = new StringBuffer();
                String str = "";
                while ((str = br.readLine()) != null) {
                    stringBuffer.append(str);
                }
                response = stringBuffer.toString();
            } else {
                response = "fail";
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放连接
            postMethod.releaseConnection();
        }
        log.info("返回结果:" + response);
        return response;
    }

    public static Map<String,String> getCookieMap(String cookieStr){
    	Map<String,String> map = new HashMap<String,String>();
    	if(cookieStr==null){
    		return map;
    	}
    	String[] cookieArray = cookieStr.split(";");
    	for (String s : cookieArray) {
    		if(s.indexOf("=")>0){
    			String[] ss = s.split("=");
    			map.put(ss[0],ss[1]);
    		}
		}
    	return map;
    }
    public static List<Cookie> getCookies(CookieStore cookieStore){
        //获取cookies信息
        List<Cookie> cookies = cookieStore.getCookies();
        return cookies;
    }
    public static List<Cookie> getCookies(HttpClientContext context){
        //获取cookies信息
        List<Cookie> cookies = context.getCookieStore().getCookies();
        return cookies;
    }
    public static Map<String,String> getHeaderMap(HttpResponse response){
    	Map<String,String> map = new HashMap<String,String>();
    	Header[] headers = response.getAllHeaders();
    	for (Header header : headers) {
    		String key = header.getName();
    		if(map.containsKey(key)){
    			map.put(key, map.get(key)+";"+header.getValue());
    		}else{
    			map.put(key, header.getValue());
    		}
		}
    	return map;
    }


    public static void debugHeader(Header[] headers) {
        if (debug) {
            for (Header header : headers) {
                log.info("header:" + header.getName() + ":" + header.getValue());
            }
        }
    }

    public static void debugHeader(HttpRequestBase httpRequest) {
        if(debug) {
            for (Header header : httpRequest.getAllHeaders()) {
                log.info("header:" + header.getName() + ":" + header.getValue());
            }
        }
    }

    public static void addHeaders(HttpRequestBase httpRequest, Map<String, String> headMap) {
        httpRequest.setHeader("Content-Type","application/x-www-form-urlencoded;charset=utf-8");
        httpRequest.setHeader(HTTP.USER_AGENT, "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36");
        httpRequest.setHeader("Accept", "application/json,text/javascript,*/*");
        for (String key : headMap.keySet()) {
            httpRequest.setHeader(key, headMap.get(key));
        }
    }

    public static String mapToJson(Map<String, Object> dataMap) {
        JSONObject js = new JSONObject(dataMap);
        return js.toString();
    }

    public static HttpResult post(String link, Map<String, Object> parameterMap,Map<String, String> headMap) throws Exception {
        log.info("请求的url:" + link);
        log.info("请求的data:" + parameterMap);
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        HttpPost httpost = new HttpPost(link.toString());
        httpost.setEntity(new StringEntity(mapToHttpString(parameterMap)));
        addHeaders(httpost, headMap);
        debugHeader(httpost);
        //发送post请求
        long beginTime = System.currentTimeMillis();
        HttpResponse response = httpclient.execute(httpost);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity,"utf-8");
        log.info("请求的返回code:" + code);
        log.info("请求的返回data:" + resultString);
        return getHttpResult(resultString, code,headers,cookies,beginTime,endTime);
    }

    public static HttpResult put(String link, Map<String, Object> dataMap, Map<String, String> headMap) throws IOException {
    	log.info("request url:" + link + " data: "+mapToJson(dataMap));
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        HttpPut httpput = new HttpPut(link.toString());

        httpput.setEntity(new StringEntity(mapToJson(dataMap), HTTP.UTF_8));
        addHeaders(httpput, headMap);
        debugHeader(httpput);
        long beiginTime = System.currentTimeMillis();
        HttpResponse response = httpclient.execute(httpput);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity);
        log.info("response code:" + code + " data:"+ resultString);
        return getHttpResult(resultString, code,headers,cookies,beiginTime,endTime);
    }

    public static HttpResult delete(String link, Map<String, Object> dataMap, Map<String, String> headMap) throws IOException {
        CookieStore cookieStore = new BasicCookieStore();
        CloseableHttpClient httpclient = HttpClients.custom().setDefaultCookieStore(cookieStore).build();
        if (dataMap != null && dataMap.size() > 0) {
          //  log.info("请求的data:" + mapToJson(dataMap));
            link += "?" + mapToHttpString(dataMap);
        }
        log.info("请求的url:" + link);
        HttpDelete httpDelete = new HttpDelete(link);
        addHeaders(httpDelete, headMap);
        debugHeader(httpDelete);
        long beiginTime = System.currentTimeMillis();
        //发送delete
        HttpResponse response = httpclient.execute(httpDelete);
        long endTime = System.currentTimeMillis();
        Map<String,String> headers = getHeaderMap(response);
        List<Cookie> cookies = getCookies(cookieStore);
        int code = response.getStatusLine().getStatusCode();// 返回状态码
        HttpEntity entity = response.getEntity();
        // 把内容转成字符串
        String resultString = EntityUtils.toString(entity);
        log.info("请求的返回code:" + code);
        log.info("请求的返回data:" + resultString);
        return getHttpResult(resultString, code, headers,cookies,beiginTime,endTime);
    }


    public static String mapToHttpString(Map<String, Object> params){
    	String dataString = "";
        if (params==null||params.isEmpty()) {
            return dataString;
        }
        try {
            for (String key : params.keySet()) {
				dataString += key + "=" + URLEncoder.encode(String.valueOf(params.get(key)),"utf-8") + "&";
            }
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
        return dataString.substring(0, dataString.length() - 1);
    }

    public static HttpResult getHttpResult(String result, int httpCode,Map<String,String> headers,List<Cookie> cookies,long beginTime,long endTime) {
        HttpResult httpResult = new HttpResult();
        httpResult.setHttpCode(httpCode);
        httpResult.setHeaders(headers);
        httpResult.setCookies(cookies);
        httpResult.setBeginTime(beginTime);
        httpResult.setEndTime(endTime);
        httpResult.setData(result);
    //    if(Tool.isJSONValid(result)) {
    //        JsonElement jsonElement = new JsonParser().parse(result);
    //        httpResult.setData(jsonElement);
    //    }else{
    //        httpResult.setData(result);
    //    }
        return httpResult;
    }
}
