package com._4paradigm.dataimporter.parseOrc;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitAll;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ParseOrcUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseOrcUtil.class);

    public static TypeDescription getSchema(String path) {
        Configuration conf = new Configuration();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(
                    new Path(path),
                    OrcFile.readerOptions(conf));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (reader != null) {
            return reader.getSchema();
        } else {
            return null;
        }
    }

    public static void putOrc(String path, String tableName, TypeDescription schema) {
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.framework.name", "local");
            conf.set("fs.defaultFS", "file:///");
            Reader reader = OrcFile.createReader(
                    new Path(path),
                    OrcFile.readerOptions(conf));
            RecordReader records = reader.rows();
            Object row = null;
            StructObjectInspector inspector
                    = (StructObjectInspector) reader.getObjectInspector();
            AtomicLong id = new AtomicLong(1);//task的id
            int index = 0;//用于选择使用哪个客户端执行put操作
            TableSyncClient client;
            while (records.hasNext()) {
                row = records.next(row);
                List<Object> valueList = inspector.getStructFieldsDataAsList(row);
                logger.debug("当前的row:{}", valueList);
                HashMap<String, Object> map = new HashMap<>();
                // 处理 schema
                for (int i = 0; i < schema.getFieldNames().size(); i++) {
                    String key = schema.getFieldNames().get(i);
                    String type = schema.getChildren().get(i).toString();
                    Object value = valueList.get(i);
                    if (type.equalsIgnoreCase("binary")) {
                        map.put(key, new String(((BytesWritable) value).getBytes()));
                    } else if (type.equalsIgnoreCase("boolean")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        if (s.equals("0")) {
                            map.put(key, false);
                        } else {
                            map.put(key, true);
                        }
                    } else if (type.equalsIgnoreCase("tinyint")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        map.put(key, Short.valueOf(s));
                    } else if (type.equalsIgnoreCase("date")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        map.put(key, Long.valueOf(s));
                    } else if (type.equalsIgnoreCase("double")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                        map.put(key, Double.valueOf(s));
                    } else if (type.equalsIgnoreCase("float")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                        map.put(key, Float.valueOf(s));
                    } else if (type.equalsIgnoreCase("int")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        map.put(key, Integer.valueOf(s));
                    } else if (type.equalsIgnoreCase("bigint")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        map.put(key, Long.valueOf(s));
                    } else if (type.equalsIgnoreCase("smallint")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                        map.put(key, Short.valueOf(s));
                    } else if (type.equalsIgnoreCase("string")) {
                        map.put(key, String.valueOf(value));
                    } else if (type.equalsIgnoreCase("timestamp")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0000-00-00 00:00:00" : String.valueOf(value);
                        map.put(key, Timestamp.valueOf(s));
                    } else if (type.substring(0, 4).equalsIgnoreCase("char")) {
                        map.put(key, String.valueOf(value));
                    } else if (type.substring(0, 7).equalsIgnoreCase("varchar")) {
                        map.put(key, String.valueOf(value));
                    } else if (type.substring(0, 7).equalsIgnoreCase("decimal")) {
                        String s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                        map.put(key, Double.valueOf(s));
                    }
                }
                if (index == InitClient.CLIENTCOUNT) {
                    index = 0;
                }
                client = InitClient.getTableSyncClient()[index];
                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), client, tableName, map));
                index++;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        InitAll.init();

        File rootFile = new File(Constant.ORC_FILEPATH);
        TypeDescription schema = null;
        List<ColumnDesc> schemaList = null;
        if (rootFile.isDirectory()) {
            List<java.nio.file.Path> filePaths = new ArrayList<>();
            File[] files = rootFile.listFiles();
            if (files == null) {
                logger.info("there is no file in the directory " + rootFile);
                return;
            }
            for (File file : files) {
                if (schemaList == null) {
                    schema = getSchema(file.toPath().toString());
                    if (schema == null) {
                        logger.info("the schema is null");
                        return;
                    }
                    schemaList = InitClient.getSchemaOfRtidb(schema);
                    InitClient.dropTable(Constant.ORC_TABLENAME);
                    InitClient.createSchemaTable(Constant.ORC_TABLENAME, schemaList);
                }
                filePaths.add(file.toPath());
                logger.info("file path is : " + file.toPath().toString());
            }
            for (java.nio.file.Path filePath : filePaths) {
                putOrc(filePath.toString(), Constant.ORC_TABLENAME, schema);
            }
        } else if (rootFile.isFile()) {
            schema = getSchema(rootFile.toPath().toString());
            if (schema == null) {
                logger.info("the schema is null");
                return;
            }
            schemaList = InitClient.getSchemaOfRtidb(schema);
            InitClient.dropTable(Constant.ORC_TABLENAME);
            InitClient.createSchemaTable(Constant.ORC_TABLENAME, schemaList);
            putOrc(rootFile.toPath().toString(), Constant.ORC_TABLENAME, schema);
        }
        InitThreadPool.getExecutor().shutdown();
        while (!InitThreadPool.getExecutor().isTerminated()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("The number of successfully inserted is : " + PutTask.successfulCount);
        logger.info("The number of unsuccessfully inserted is : " + PutTask.unSuccessfulCount);
    }
}
