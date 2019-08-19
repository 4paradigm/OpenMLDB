package com._4paradigm.dataimporter.example;

import com._4paradigm.dataimporter.initialization.InitAll;
import com._4paradigm.dataimporter.parseutil.ParseOrcUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Test {
    private static Logger logger = LoggerFactory.getLogger(Test.class);
    private static String filePath = "/Users/innerpeace/Desktop/test.orc";
    private static String tableName = "orcTest";
    private long timestamp;
    private TypeDescription schema;

    public Test(String path, String tableName, TypeDescription schema) {
        this.filePath = path;
        this.tableName = tableName;
        this.schema = schema;
    }

    public static void writeOrc() {
        try {
            TypeDescription schema = TypeDescription.createStruct()
                    .addField("binary_1", TypeDescription.createBinary())
                    .addField("boolean_1", TypeDescription.createBoolean())
                    .addField("byte_1", TypeDescription.createByte())
                    .addField("date_1", TypeDescription.createDate())
                    .addField("double_1", TypeDescription.createDouble())
                    .addField("float_1", TypeDescription.createFloat())
                    .addField("int_1", TypeDescription.createInt())
                    .addField("long_1", TypeDescription.createLong())
                    .addField("short_1", TypeDescription.createShort())
                    .addField("string_1", TypeDescription.createString())
                    .addField("timestamp_1", TypeDescription.createTimestamp())
                    .addField("char_1", TypeDescription.createChar().withMaxLength(128))
                    .addField("varchar_1", TypeDescription.createVarchar())
                    .addField("decimal_1", TypeDescription.createDecimal());
            Configuration conf = new Configuration();
            FileSystem.getLocal(conf);
            Writer writer = OrcFile.createWriter(new Path(filePath),
                    OrcFile.writerOptions(conf).setSchema(schema));
            //要写入的内容
            String[] contents = new String[]{"12910789", "1", "4", "11", "1.0", "1.0", "1", "1000001", "1", "ss", "2018-10-08 20:12:12", "1", "aa", "2.0"};

            VectorizedRowBatch batch = schema.createRowBatch();
            int rowCount;
            for (int j = 0; j < 1; j++) {

                rowCount = batch.size++;
                ((BytesColumnVector) batch.cols[0]).setVal(rowCount, contents[0].getBytes());
                ((LongColumnVector) batch.cols[1]).vector[rowCount] = Long.parseLong(contents[1]);
                ((LongColumnVector) batch.cols[2]).vector[rowCount] = Long.parseLong(contents[2]);
                ((LongColumnVector) batch.cols[3]).vector[rowCount] = Long.parseLong(contents[3]);
                ((DoubleColumnVector) batch.cols[4]).vector[rowCount] = Double.parseDouble(contents[4]);
                ((DoubleColumnVector) batch.cols[5]).vector[rowCount] = Double.parseDouble(contents[5]);
                ((LongColumnVector) batch.cols[6]).vector[rowCount] = Long.parseLong(contents[6]);
                ((LongColumnVector) batch.cols[7]).vector[rowCount] = Long.parseLong(contents[7]);
                ((LongColumnVector) batch.cols[8]).vector[rowCount] = Long.parseLong(contents[8]);
                ((BytesColumnVector) batch.cols[9]).setVal(rowCount, contents[9].getBytes());
                ((TimestampColumnVector) batch.cols[10]).set(rowCount, Timestamp.valueOf(contents[10]));
                ((BytesColumnVector) batch.cols[11]).setVal(rowCount, contents[11].getBytes());
                ((BytesColumnVector) batch.cols[12]).setVal(rowCount, contents[12].getBytes());
                ((DecimalColumnVector) batch.cols[13]).vector[rowCount] = new HiveDecimalWritable(contents[13]);
                //batch full
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


//    public static void get(){
//        ////////////////////////////////////////////
//        Object[] result = new Object[0];
//        try {
//            result = InitClient.getTableSyncClient()[0].getRow(tableName, "1", "int_1", 0);
//        } catch (TimeoutException e) {
//            e.printStackTrace();
//        } catch (TabletException e) {
//            e.printStackTrace();
//        }
//        System.out.println("3:" + result[0].toString().length());
//        System.out.println("3:" + result[0].toString().equals("129 "));
//        ////////////////////////////////////////////
//    }

    public HashMap<String, Object> read(StructObjectInspector inspector, Object row) {
        HashMap<String, Object> map = new HashMap<>();
        List<Object> valueList = inspector.getStructFieldsDataAsList(row);
        logger.debug("row:{}", valueList);
        int index;
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            index = i;
            String columnName = schema.getFieldNames().get(i);
            TypeDescription.Category columnType = schema.getChildren().get(i).getCategory();
            Object value = valueList.get(index);
            String s;
            switch (columnType) {
                case BINARY:
                    map.put(columnName, new String(((BytesWritable) value).copyBytes()));
                    break;
                case BOOLEAN:
                    s = StringUtils.isBlank(String.valueOf(value)) ? null : String.valueOf(value);
                    if (s == null) {
                        map.put(columnName, false);
                    } else {
                        map.put(columnName, Boolean.parseBoolean(s));
                    }
                    break;
                case BYTE:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Short.valueOf(s));
                    break;
                case DATE:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, ((DateWritable) value).get());
                    break;
                case DOUBLE:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                    map.put(columnName, Double.valueOf(s));
                    break;
                case FLOAT:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                    map.put(columnName, Float.valueOf(s));
                    break;
                case INT:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Integer.valueOf(s));
                    break;
                case LONG:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Long.valueOf(s));
                    break;
                case SHORT:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0" : String.valueOf(value);
                    map.put(columnName, Short.valueOf(s));
                    break;
                case STRING:
                    map.put(columnName, String.valueOf(value));
                    break;
                case TIMESTAMP:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0000-00-00 00:00:00" : String.valueOf(value);
                    map.put(columnName, Timestamp.valueOf(s));
                    break;
                case CHAR:
                    map.put(columnName, String.valueOf(value));
                    break;
                case VARCHAR:
                    map.put(columnName, String.valueOf(value));
                    break;
                case DECIMAL:
                    s = StringUtils.isBlank(String.valueOf(value)) ? "0.0" : String.valueOf(value);
                    map.put(columnName, Double.valueOf(s));
                    break;
                default:
            }
        }
        return map;
    }

    public void put() {
        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.framework.name", "local");
            conf.set("fs.defaultFS", "file:///");
            Reader reader = OrcFile.createReader(
                    new Path(filePath),
                    OrcFile.readerOptions(conf));
            RecordReader records = reader.rows();
            Object row = null;
            StructObjectInspector inspector
                    = (StructObjectInspector) reader.getObjectInspector();
            AtomicLong id = new AtomicLong(1);//task的id
//            int clientIndex = 0;//用于选择使用哪个客户端执行put操作
            while (records.hasNext()) {
                row = records.next(row);
                HashMap<String, Object> map = read(inspector, row);
                System.out.println("the map is : " + map);
                Iterator<Map.Entry<String, Object>> itor = map.entrySet().iterator();
                while (itor.hasNext()) {
                    Map.Entry entry = itor.next();
                    System.out.print(entry.getKey()+" : ");
                    System.out.println(entry.getValue());
                }
//                if (clientIndex == InitClient.MAX_THREAD_NUM) {
//                    clientIndex = 0;
//                }
//                TableSyncClient client = InitClient.getTableSyncClient()[clientIndex];
//                timestamp = System.currentTimeMillis();

//                InitThreadPool.getExecutor().submit(new PutTask(String.valueOf(id.getAndIncrement()), timestamp, client, tableName, map));
//                clientIndex++;

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        writeOrc();
        InitAll.init();
        Test test = new Test(filePath, tableName, ParseOrcUtil.getSchema(filePath));
        test.put();



//        java.util.Date dt = new Date();
//        System.out.println(dt.toString());   //java.util.Date的含义
//        long lSysTime1 = dt.getTime();   //得到秒数，Date类型的getTime()返回毫秒数
//        System.out.println(lSysTime1);
//        System.out.println(getSchema().getCategory());
//        System.out.println(getSchema().getChildren().get(0).toString());
//        System.out.println(getSchema().getFieldNames());
//        System.out.println(getSchema().getId());
//        putOrc();
//        System.out.println(new String(new String("1").getBytes()));
//        get();
    }
}






