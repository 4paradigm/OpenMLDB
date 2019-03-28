package com._4paradigm.dataimporter.parseParquet;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com._4paradigm.dataimporter.initialization.InitAll;
import com._4paradigm.dataimporter.initialization.InitProperties;
import com._4paradigm.dataimporter.initialization.OperateTable;
import com._4paradigm.dataimporter.initialization.OperateThreadPool;
import com._4paradigm.dataimporter.task.PutTask;
import com._4paradigm.dataimporter.verification.CheckParameters;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

public class ParseParquetUtil {
    private static Logger logger = LoggerFactory.getLogger(ParseParquetUtil.class);
//    private static String parquetPath = "file:///Users/innerpeace/Desktop/test.parq";//file://是文件协议，可以不加
    private static String parquetPath = "/home/wangbao/test.parq";//file://是文件协议，可以不加
    private static String tableName = "parquetTest";

    /**
     * @param path  文件的绝对路径
     * @param tableName  表名
     * @param schemaList  保存schema的list
     */
    public static void putParquet(String path,String tableName, List<ColumnDesc> schemaList) {
        if (CheckParameters.checkPutParameters(path, tableName, schemaList)) return;
        SimpleGroup group;
        AtomicLong id = new AtomicLong(1);//task的id
        int index=0;//用于选择使用哪个客户端执行put操作
        TableSyncClient client;
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), new Path(path));
        try {
            ParquetReader<Group> reader = builder.build();
            while ((group = (SimpleGroup) reader.read()) != null) {
                HashMap<String, Object> map = new HashMap<>();
                String columnName ;
                for (int i = 0; i < schemaList.size(); i++) {
                    columnName = schemaList.get(i).getName();
                    if (columnName.equals("int_32")) {
                        map.put(columnName, group.getInteger(i, 0));
                    } else if (columnName.equals("int_64")) {
                        map.put(columnName, group.getLong(i, 0));
                    } else if (columnName.equals("int_96")) {
                        map.put(columnName, new String(group.getInt96(i, 0).getBytes()));
                    } else if (columnName.equals("float_1")) {
                        map.put(columnName, group.getFloat(i, 0));
                    } else if (columnName.equals("double_1")) {
                        map.put(columnName, group.getDouble(i, 0));
                    } else if (columnName.equals("boolean_1")) {
                        map.put(columnName, group.getBoolean(i, 0));
                    } else if (columnName.equals("binary_1")) {
                        map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
                    }
                }
                if(index== OperateTable.COUNT){
                    index=0;
                }
                client = OperateTable.getTableSyncClient()[index];
                OperateThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), client, tableName, map));
                index++;
            }
            OperateThreadPool.getExecutor().shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * @param path
     * @return
     * @throws IOException
     * @description 读取parquet文件获取schema
     */
    public static MessageType getSchema(Path path) throws IOException {
        Configuration configuration = new Configuration();
//         windows 下测试入库impala需要这个配置
//        System.setProperty("hadoop.home.dir",
//                "E:\\mvtech\\software\\hadoop-common-2.2.0-bin-master");
        ParquetMetadata readFooter = ParquetFileReader.readFooter(configuration,
                path, ParquetMetadataConverter.NO_FILTER);
        return readFooter.getFileMetaData().getSchema();
    }


    /**
     * @param string
     * @return
     * @description 得到基本数据类型的字符串
     */
    public static String getDataType(String string) {
        String[] array = string.split(" ");
        return array[1];
    }

    public static void main(String[] args) {
        InitAll.init();
//        OperateTable.dropTable(tableName);
//        OperateTable.createSchemaTable(tableName, OperateTable.getRtidbSchema());
        putParquet(parquetPath,tableName, OperateTable.getRtidbSchema());

    }
}

