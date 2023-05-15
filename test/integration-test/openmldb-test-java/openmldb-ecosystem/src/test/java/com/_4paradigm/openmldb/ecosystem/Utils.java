package com._4paradigm.openmldb.ecosystem;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.io.CharStreams;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.StreamSupport;

import org.testng.Assert;

public class Utils {
    static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    public static String kafkaConnectorUrl(Properties properties, String connectorName) {
        String url = properties.getProperty("connect.listeners");
        if (url == null || url.isEmpty()) {
            url = "http://localhost:8083";
        }
        // default is 8083
        return url + "/connectors/" + connectorName;
    }

    public static String kafkaConnectorCreate(String connectorUrl, String config) throws IOException {
        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory();
        HttpContent content = ByteArrayContent.fromString("application/json", config);
        HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(connectorUrl), content);
        HttpResponse response = request.execute();
        // TODO check response.getStatusMessage());
        return CharStreams.toString(new InputStreamReader(response.getContent()));
    }

    public static String kafkaConnectorDelete(String connectorUrl) throws IOException {
        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory();
        HttpRequest request = requestFactory.buildDeleteRequest(new GenericUrl(connectorUrl));
        HttpResponse response = request.execute();
        // TODO check response.getStatusMessage());
        return CharStreams.toString(new InputStreamReader(response.getContent()));
    }

    public static String kafkaConnectorUpdate(String connectorUrl, String config) throws IOException {
        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory();
        HttpContent content = ByteArrayContent.fromString("application/json", config);
        HttpRequest request = requestFactory.buildPutRequest(new GenericUrl(connectorUrl), content);
        HttpResponse response = request.execute();
        // TODO check response.getStatusMessage());
        return CharStreams.toString(new InputStreamReader(response.getContent()));
    }

    public static String apiserverQuery(String apiserverAddr, String db, String sql)
            throws IOException {
        // mode is online
        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory();
        HttpContent content = ByteArrayContent.fromString(
                "application/json", "{\"sql\":\"" + sql + "\",\"mode\":\"online\"}");
        HttpRequest request = requestFactory.buildPostRequest(
                new GenericUrl("http://" + apiserverAddr + "/dbs/" + db), content);
        HttpResponse response = request.execute();
        // TODO check response.getStatusMessage());
        return CharStreams.toString(new InputStreamReader(response.getContent()));
    }

    public static String apiserverRefresh(String apiserverAddr) throws IOException {
        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory();
        HttpRequest request = requestFactory.buildPostRequest(
                new GenericUrl("http://" + apiserverAddr + "/refresh"), null);
        HttpResponse response = request.execute();
        // TODO check response.getStatusMessage());
        return CharStreams.toString(new InputStreamReader(response.getContent()));
    }

    public static boolean apiserverTableExists(String apiserverAddr, String db, String table) throws IOException {
        HttpRequestFactory requestFactory = HTTP_TRANSPORT.createRequestFactory();
        HttpRequest request = requestFactory.buildPostRequest(
                new GenericUrl("http://" + apiserverAddr + "/dbs/" + db + "/tables"), null);
        HttpResponse response = request.execute();
        // TODO check response.getStatusMessage());
        String res = CharStreams.toString(new InputStreamReader(response.getContent()));
        // to json and check if table exists
        JsonObject resJson = JsonParser.parseString(res).getAsJsonObject();
        Assert.assertTrue(resJson.get("code").getAsInt() == 0, "fail to get table list");
        boolean find = false;
        Iterator<JsonElement> iter = resJson.get("tables").getAsJsonArray().iterator();
        Iterable<JsonElement> iterable = () -> iter;
        return StreamSupport.stream(iterable.spliterator(), false)
                .anyMatch(e -> e.getAsJsonObject().get("name").getAsString().equals(table));
    }
}
