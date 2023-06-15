package com._4paradigm.openmldb.ecosystem;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class TestKafka {
    private Map<String, Object> configMap;
    private Properties properties;
    private Properties producerProperties;
    private String apiserverAddr;

    @BeforeTest
    public void setUp() throws IOException {
        // load config file
        Yaml yaml = new Yaml();
        InputStream inputStream = TestKafka.class.getClassLoader().getResourceAsStream("kafka_test_cases.yml");
        configMap = yaml.load(inputStream);

        Map<String, Object> kafka = (Map<String, Object>) configMap.get("kafka");
        // admin client needs bootstrap.servers
        // connect client needs connect.listeners
        properties = new Properties();
        properties.setProperty("bootstrap.servers", kafka.get("bootstrap.servers").toString());
        properties.setProperty("connect.listeners", kafka.get("connect.listeners").toString());
        producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", properties.getProperty("bootstrap.servers"));
        // a map
        ((Map<String, String>) kafka.get("producer.properties"))
                .forEach((k, v) -> producerProperties.setProperty(k, v));

        // read openmldb apiserver for admin
        Map<String, String> openmldb = (Map<String, String>) configMap.get("openmldb");
        apiserverAddr = openmldb.get("apiserver.address");
        Assert.assertTrue(apiserverAddr != null, "apiserver.address is not set");
        log.info("kafka properties: {}, producer {}, openmldb apiserver: {}", properties, producerProperties,
                apiserverAddr);
        // TODO(hw): kafka test cluster
        // https://mvnrepository.com/artifact/org.testcontainers/kafka
    }

    @SuppressWarnings("unchecked")
    @DataProvider(name = "case-provider")
    public Object[][] testCases() {
        // read kafka test cases
        List<Object> cases = (List<Object>) configMap.get("cases");
        // add common_connector_conf to each case
        cases.forEach(c -> {
            Map<String, Object> caseMap = (Map<String, Object>) c;
            caseMap.put("common_connector_conf", configMap.get("common_connector_conf"));
        });
        Object[][] rowArray = new Object[cases.size()][];
        for (int i = 0; i < cases.size(); i++) {
            rowArray[i] = new Object[] { cases.get(i) };
        }
        return rowArray;
    }

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "case-provider")
    public void runTest(Object ut) throws IOException, ParseException {
        log.info("run test: {}", ut);
        Map<String, Object> caseMap = (Map<String, Object>) ut;

        // create a sink connector to OpenMLDB by http api
        Map<String, Object> appendConf = (Map<String, Object>) caseMap.get("append_conf");
        Map<String, Object> commonConf = (Map<String, Object>) caseMap.get("common_connector_conf");
        // use append to override common
        commonConf.putAll(appendConf);
        String config = new Gson().toJson(commonConf);
        log.info("config {}", config);

        // only support one topic
        String topicName = (String) commonConf.get("topics");
        try (Admin admin = Admin.create(properties)) {
            // deleteTopics first to avoid extra data in topic
            DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(topicName));
            result.all().get();
            log.info("current kafka topics: {}",
                    admin.listTopics().names().get().stream().collect(Collectors.toList()));
        } catch (InterruptedException | ExecutionException e) {
            log.info("topic {} delete failed, {}", topicName, e.getMessage());
            // TODO(hw): what if topic still exists?
        }
        // when no topic, create the connector will create the topic, it's ok in test

        // read connector name from file
        String connectorName = (String) commonConf.get("name");
        // delete to avoid conflict
        try {
            Utils.kafkaConnectorDelete(Utils.kafkaConnectorUrl(properties, connectorName));
        } catch (Exception e) {
            log.info("delete connector simple-connector failed: {}", e.getMessage());
        }

        try {
            String createJson = "{\"name\":\"" + connectorName + "\",\"config\": " + config + "}";
            String ret = Utils.kafkaConnectorCreate(Utils.kafkaConnectorUrl(properties, ""), createJson);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // get db from config
        JsonObject connectorConfigJson = JsonParser.parseString(config).getAsJsonObject();
        String jdbcUrl = connectorConfigJson.get("connection.url").getAsString();
        // get dbname from url jdbc:openmldb:///<db>?
        String dbName = jdbcUrl.substring(17, jdbcUrl.lastIndexOf("?"));
        // TODO: ApiServer can't create database, you should create database first?

        // ensure openmldb db exists
        Utils.apiserverQuery(apiserverAddr, "foo", "CREATE DATABASE IF NOT EXISTS " + dbName);
        // just drop test table to avoid unexpected data, if no openmldb_ddl, kafka
        // connector will create table automatically
        String testTable = (String) ((Map<String, Object>) caseMap.get("expect")).get("table");
        // don't handle drop result, table may not exists before
        Utils.apiserverQuery(apiserverAddr, dbName, "DROP TABLE " + testTable);
        // check no table here
        Assert.assertFalse(Utils.apiserverTableExists(apiserverAddr, dbName, testTable));
        String createDDL = (String) caseMap.get("openmldb_ddl");
        if (createDDL != null) {
            // create table in OpenMLDB
            String ret = Utils.apiserverQuery(apiserverAddr, dbName, createDDL);
            log.info("create table ret: {}", ret);
        }

        // send data to kafka, get input msgs from yaml
        List<Map<String, Object>> msgs = (List<Map<String, Object>>) caseMap.get("messages");
        try (final Producer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            for (Map<String, Object> msg : msgs) {
                // only support json style
                String value = new Gson().toJson(msg.get("json"));
                log.info("produce msg to kafka: {}", value);
                producer.send(new ProducerRecord<>(topicName, null, value), (event, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        log.info("Produced event to topic {}: event {}", topicName, event.offset());
                });
            }
        }
        // check in OpenMLDB
        try {
            // sleep to wait for data sync, if get empty result, but you can get data in
            // CLI, increase
            // sleep time
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Utils.apiserverRefresh(apiserverAddr);

        String tableContent = Utils.apiserverQuery(apiserverAddr, dbName, "select * from " + testTable);
        Assert.assertNotNull(tableContent, "query OpenMLDB failed");
        log.info("table {} get content: {}", testTable, tableContent);
        JsonObject resultJson = JsonParser.parseString(tableContent).getAsJsonObject();
        // check code
        Assert.assertEquals(resultJson.get("code").getAsInt(), 0, "response " + tableContent);
        // check data
        JsonArray result = resultJson.getAsJsonObject("data").getAsJsonArray("data");
        List<Object> expect = (List<Object>) ((Map<String, Object>) caseMap.get("expect")).get("data");
        Assert.assertEquals(result.size(), expect.size(), "result size not match");
        for (int i = 0; i < result.size(); i++) {
            JsonArray row = result.get(i).getAsJsonArray();
            List<Object> expectRow = (List<Object>) expect.get(i);
            Assert.assertEquals(row.size(), expectRow.size(), "row size not match");
            for (int j = 0; j < row.size(); j++) {
                Assert.assertEquals(
                        row.get(j).getAsString(), expectRow.get(j).toString(), "row content not match");
            }
        }
    }
}
