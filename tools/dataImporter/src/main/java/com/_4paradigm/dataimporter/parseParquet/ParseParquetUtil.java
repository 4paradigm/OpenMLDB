package com._4paradigm.dataimporter.parseParquet;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com._4paradigm.dataimporter.initialization.Constant;
import com._4paradigm.dataimporter.initialization.InitAll;
import com._4paradigm.dataimporter.initialization.InitClient;
import com._4paradigm.dataimporter.initialization.InitThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.dataimporter.verification.CheckParameters;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;

public class ParseParquetUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseParquetUtil.class);

    /**
     * @param path
     * @param tableName
     * @param schema
     */
    public static void putParquet(String path, String tableName, MessageType schema) {
        if (CheckParameters.checkPutParameters(path, tableName, schema)) return;
        SimpleGroup group;
        AtomicLong id = new AtomicLong(1);//task的id
        int index = 0;//用于选择使用哪个客户端执行put操作
        TableSyncClient client;
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(path));
        try {
            ParquetReader<Group> reader = builder.build();
            while ((group = (SimpleGroup) reader.read()) != null) {
                HashMap<String, Object> map = new HashMap<>();
                String columnName;
                PrimitiveType.PrimitiveTypeName type;
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    columnName = schema.getFieldName(i);
                    type = schema.getType(i).asPrimitiveType().getPrimitiveTypeName();
                    if (type.equals(PrimitiveType.PrimitiveTypeName.INT32)) {
                        map.put(columnName, group.getInteger(i, 0));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
                        map.put(columnName, group.getLong(i, 0));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.INT96)) {
                        map.put(columnName, new String(group.getInt96(i, 0).getBytes()));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.FLOAT)) {
                        map.put(columnName, group.getFloat(i, 0));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.DOUBLE)) {
                        map.put(columnName, group.getDouble(i, 0));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)) {
                        map.put(columnName, group.getBoolean(i, 0));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
                        map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
                    } else if (type.equals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
                        map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
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
        InitClient.dropTable(Constant.PARQUET_TABLENAME);

        File rootFile = new File(Constant.PARQUET_FILEPATH);
        List<ColumnDesc> schemaList = null;
        MessageType schema = null;
        if (rootFile.isDirectory()) {
            List<java.nio.file.Path> filePaths = new ArrayList<>();
            File[] files = rootFile.listFiles();
            if (files == null) {
                logger.info("there is no file in the directory " + rootFile);
                return;
            }
            for (File file : files) {
                if (schemaList == null) {
                    schema = InitClient.getSchema(new Path(file.toPath().toString()));
                    if (schema == null) {
                        logger.info("the schema is null");
                        return;
                    }
                    schemaList = InitClient.getSchemaOfRtidb(schema);
                    InitClient.createSchemaTable(Constant.PARQUET_TABLENAME, schemaList);
                }
                filePaths.add(file.toPath());
                logger.info("file path is : " + file.toPath().toString());
            }
            for (java.nio.file.Path filePath : filePaths) {
                putParquet(filePath.toString(), Constant.PARQUET_TABLENAME, schema);
            }
        } else if (rootFile.isFile()) {
            schema = InitClient.getSchema(new Path(rootFile.toPath().toString()));
            if (schema == null) {
                logger.info("the schema is null");
                return;
            }
            schemaList = InitClient.getSchemaOfRtidb(schema);
            InitClient.createSchemaTable(Constant.PARQUET_TABLENAME, schemaList);
            putParquet(rootFile.toPath().toString(), Constant.PARQUET_TABLENAME, schema);
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

