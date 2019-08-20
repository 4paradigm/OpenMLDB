package com._4paradigm.dataimporter.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;

public class OrcExample {
    private static Logger logger = LoggerFactory.getLogger(OrcExample.class);
    private static String path = "/Users/innerpeace/Desktop/test.orc";
    private static String tableName = "orcTest";

    public static void writeOrc() {
        try {
//            TypeDescription schema = TypeDescription.createStruct()
//                    .addField("binary_1", TypeDescription.createBinary())
//                    .addField("boolean_1", TypeDescription.createBoolean())
//                    .addField("byte_1", TypeDescription.createByte())
//                    .addField("date_1", TypeDescription.createDate())
//                    .addField("double_1", TypeDescription.createDouble())
//                    .addField("float_1", TypeDescription.createFloat())
//                    .addField("int_1", TypeDescription.createInt())
//                    .addField("long_1", TypeDescription.createLong())
//                    .addField("short_1", TypeDescription.createShort())
//                    .addField("string_1", TypeDescription.createString())
//                    .addField("timestamp_1", TypeDescription.createTimestamp())
//                    .addField("char_1", TypeDescription.createChar().withMaxLength(128))
//                    .addField("varchar_1", TypeDescription.createVarchar())
//                    .addField("decimal_1", TypeDescription.createDecimal());
            TypeDescription schema = TypeDescription.createStruct()
                    .addField("card", TypeDescription.createString())
                    .addField("mcc", TypeDescription.createString())
                    .addField("amt", TypeDescription.createFloat())
                    .addField("ts1", TypeDescription.createLong())
                    .addField("ts2", TypeDescription.createTimestamp());
            Configuration conf = new Configuration();
            FileSystem.getLocal(conf);
            Writer writer = OrcFile.createWriter(new Path(path),
                    OrcFile.writerOptions(conf).setSchema(schema));
//            //要写入的内容
//            String[] contents = new String[]{"12910789", "1", "4", "100000", "1.0", "1.0", "1", "1000001", "1", "ss", "2018-10-08 20:12:12", "1", "aa", "2.0"};
//
//            VectorizedRowBatch batch = schema.createRowBatch();
//            int rowCount;
//            for (int j = 0; j < 5; j++) {
//                rowCount = batch.size++;
//                ((BytesColumnVector) batch.cols[0]).setVal(rowCount, contents[0].getBytes());
//                ((LongColumnVector) batch.cols[1]).vector[rowCount] = Long.parseLong(contents[1]);
//                ((LongColumnVector) batch.cols[2]).vector[rowCount] = Long.parseLong(contents[2]);
//                ((LongColumnVector) batch.cols[3]).vector[rowCount] = Long.parseLong(contents[3]);
//                ((DoubleColumnVector) batch.cols[4]).vector[rowCount] = Double.parseDouble(contents[4]);
//                ((DoubleColumnVector) batch.cols[5]).vector[rowCount] = Double.parseDouble(contents[5]);
//                ((LongColumnVector) batch.cols[6]).vector[rowCount] = Long.parseLong(contents[6]);
//                ((LongColumnVector) batch.cols[7]).vector[rowCount] = Long.parseLong(contents[7]);
//                ((LongColumnVector) batch.cols[8]).vector[rowCount] = Long.parseLong(contents[8]);
//                ((BytesColumnVector) batch.cols[9]).setVal(rowCount, contents[9].getBytes());
//                ((TimestampColumnVector) batch.cols[10]).set(rowCount, Timestamp.valueOf(contents[10]));
//                ((BytesColumnVector) batch.cols[11]).setVal(rowCount, contents[11].getBytes());
//                ((BytesColumnVector) batch.cols[12]).setVal(rowCount, contents[12].getBytes());
//                ((DecimalColumnVector) batch.cols[13]).vector[rowCount] = new HiveDecimalWritable(contents[13]);
//                //batch full
//                if (batch.size == batch.getMaxSize()) {
//                    writer.addRowBatch(batch);
//                    batch.reset();
//                }
            //要写入的内容
            String[] contents = new String[]{"60001", "101", "-20", "1563525301439", "2018-11-11 12:12:13"};

            VectorizedRowBatch batch = schema.createRowBatch();
            int rowCount;
            for (int j = 0; j < 5; j++) {
                rowCount = batch.size++;
                ((BytesColumnVector) batch.cols[0]).setVal(rowCount, contents[0].getBytes());
                ((BytesColumnVector) batch.cols[1]).setVal(rowCount, contents[1].getBytes());
                ((DoubleColumnVector) batch.cols[2]).vector[rowCount] = Double.parseDouble(contents[2]);
                ((LongColumnVector) batch.cols[3]).vector[rowCount] = Long.parseLong(contents[3]);
                ((TimestampColumnVector) batch.cols[4]).set(rowCount, Timestamp.valueOf(contents[4]));
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


    public static void main(String[] args) {
        writeOrc();
//        System.out.println(getSchema().getCategory());
//        System.out.println(getSchema().getChildren().get(0).toString());
//        System.out.println(getSchema().getFieldNames());
//        System.out.println(getSchema().getId());
//        putOrc();
//        System.out.println(new String(new String("1").getBytes()));
//        get();
    }
}






