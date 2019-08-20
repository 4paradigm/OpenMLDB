package com._4paradigm.dataimporter.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.JulianFields;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @描述：测试parquet读写类
 */
public class ParquetExample {

    private static Logger logger = LoggerFactory
            .getLogger(ParquetExample.class);
//    private static String schemaStr = "message schema {"
//            + "optional int32 int_32;"
//            + "optional int64 int_64;"
//            + "optional int96 int_96;"
//            + "optional float float_1;"
//            + "optional double double_1;"
//            + "optional boolean boolean_1;"
//            + "optional binary binary_1;}";

    private static String schemaStr = "message schema {"
            + "optional binary card;"
            + "optional binary mcc;"
            + "optional float amt;"
            + "optional int64 ts1;"
            + "optional int96 ts2;}";

    static MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

    /**
     * @描述：输出MessageType
     */
    public static void testParseSchema() {
        logger.info(schema.toString());
    }

    /**
     * @throws Exception
     * @描述：获取parquet的Schema
     */
    public static void testGetSchema() throws Exception {
        Configuration configuration = new Configuration();
        // windows 下测试入库impala需要这个配置
//        System.setProperty("hadoop.home.dir",
//                "E:\\mvtech\\software\\hadoop-common-2.2.0-bin-master");
        ParquetMetadata readFooter = null;
        Path parquetFilePath = new Path("file:///Users/innerpeace/Desktop/test.parquet");
        readFooter = ParquetFileReader.readFooter(configuration,
                parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = readFooter.getFileMetaData().getSchema();
        logger.info(schema.toString());
    }

    /**
     * @throws IOException
     * @描述：测试写parquet文件
     */
    private static void parquetWriter() throws IOException, ParseException {
        Path file = new Path("/Users/innerpeace/Desktop/test.parquet");
        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(file).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                //.withConf(configuration)
                .withType(schema);
        /////////////////
        String value = "2019-02-13 13:35:05";

        final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
        final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
        final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

        // Parse date
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(parser.parse(value));

        // Calculate Julian days and nanoseconds in the day
        LocalDate dt = LocalDate.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH));
        int julianDays = (int) JulianFields.JULIAN_DAY.getFrom(dt);
        long nanos = (cal.get(Calendar.HOUR_OF_DAY) * NANOS_PER_HOUR)
                + (cal.get(Calendar.MINUTE) * NANOS_PER_MINUTE)
                + (cal.get(Calendar.SECOND) * NANOS_PER_SECOND);

        // Write INT96 timestamp
        byte[] timestampBuffer = new byte[12];
        ByteBuffer buf = ByteBuffer.wrap(timestampBuffer);
        buf.order(ByteOrder.LITTLE_ENDIAN).putLong(nanos).putInt(julianDays);

        // This is the properly encoded INT96 timestamp
        Binary tsValue = Binary.fromReusedByteArray(timestampBuffer);


        ///////////////
        /*
         * file, new GroupWriteSupport(), CompressionCodecName.SNAPPY, 256 *
         * 1024 * 1024, 1 * 1024 * 1024, 512, true, false,
         * ParquetProperties.WriterVersion.PARQUET_1_0, conf
         */
        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
//        String[] arr = {"1", "22222", "000010000100", "444444", "55555", "true", "100001000010000", "1556455105503"};
//        byte[] brr = {'1', '1', '2', '3', '0', '1', '2', '3', '0', '1', '2', '3'};
//        for (int i = 0; i < 2; i++) {
//            writer.write(groupFactory.newGroup()
//                    .append("int_32", Integer.parseInt(arr[0]))
//                    .append("int_64", Long.parseLong(arr[1] + i))
//                    .append("int_96", tsValue)
//                    .append("float_1", Float.parseFloat(arr[3] + i))
//                    .append("double_1", Double.parseDouble(arr[4] + i))
//                    .append("boolean_1", Boolean.parseBoolean(arr[5]))
//                    .append("binary_1", arr[6]));
//
//        }
        String[] arr = {"60001", "101", "-20", "1563525301439", "2018-11-11 12:12:13"};
        byte[] brr = {'1', '1', '2', '3', '0', '1', '2', '3', '0', '1', '2', '3'};
        for (int i = 0; i < 2; i++) {
            writer.write(groupFactory.newGroup()
                    .append("card", arr[0])
                    .append("mcc", arr[1])
                    .append("amt", Float.parseFloat(arr[2]))
                    .append("ts1", Long.parseLong(arr[3]))
                    .append("ts2", tsValue));

        }
        writer.close();
    }

    /**
     * @throws IOException
     * @描述：测试读parquet文件
     */
    private static void parquetReader() throws IOException {
        Path file = new Path("file:///Users/innerpeace/Desktop/test.parq");
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);
        ParquetReader<Group> reader = builder.build();
        SimpleGroup group = null;
        int i = 0;
        while ((group = (SimpleGroup) reader.read()) != null) {
            logger.info("schema:" + group.getType());
            logger.info("idc_id:" + group.getValueToString(0, 0));
            System.out.println(++i);
        }
    }


    /**
     * @param path parquet文件的绝对路径
     * @throws IOException
     */
//    public static void parseParquet(String path) throws IOException {
//        Path file = new Path(path);
//        MessageType schema = getSchema(file);
//        if (schema == null) {
//            logger.error("method getSchema() failed");
//            throw new IOException("method getSchema() failed");
//        }
//        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file);
//        ParquetReader<Group> reader = builder.build();
//        SimpleGroup group = null;
//        HashMap<String, Object> map = new HashMap<>();
//        String dataType = null;
//        String columnName = null;
//        PutFuture pf = null;
//
//        while ((group = (SimpleGroup) reader.read()) != null) {
//            for (int i = 0; i < schema.getFieldCount(); i++) {
//                dataType = getDataType(group.getType().getType(i).toString());
//                columnName = group.getType().getFieldName(i);
//                if (dataType.equals("int32")) {
//                    map.put(columnName, group.getInteger(i, 0));
//                } else if (dataType.equals("int64")) {
//                    map.put(columnName, group.getLong(i, 0));
//                } else if (dataType.equals("int96")) {
//                    map.put(columnName, new String(group.getInt96(i, 0).getBytes()));
//                } else if (dataType.equals("float")) {
//                    map.put(columnName, group.getFloat(i, 0));
//                } else if (dataType.equals("double")) {
//                    map.put(columnName, group.getDouble(i, 0));
//                } else if (dataType.equals("boolean")) {
//                    map.put(columnName, String.valueOf(group.getBoolean(i, 0)));
//                } else if (dataType.equals("binary")) {
//                    map.put(columnName, new String(group.getBinary(i, 0).getBytes()));
//                }
//            }
//
//            try {
//                tableAsyncClient.put(tableName, System.currentTimeMillis(), map);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            map.clear();
//
//            }
//        }

    /**
     * 创建时间：2017-8-2 创建者：meter 返回值类型：void
     *
     * @param args
     * @throws Exception
     * @描述：
     */
    public static void main(String[] args) throws Exception {

//        testParseSchema();
        parquetWriter();
//        testGetSchema();
//        parquetReader();
//        System.out.println(Byte.parseByte("1"));
        System.out.println(schema.getType(2));
        System.out.println(schema.getFieldName(2));
        System.out.println(schema.getType(2).getOriginalType());
        System.out.println(schema.getType(2).asPrimitiveType().getPrimitiveTypeName());
    }

    public static String getDataType(String string) {
        String[] array = string.split(" ");
        return array[1];
    }
}