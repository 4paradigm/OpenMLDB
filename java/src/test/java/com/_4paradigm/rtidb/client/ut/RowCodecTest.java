package com._4paradigm.rtidb.client.ut;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.schema.*;
import com._4paradigm.rtidb.type.Type.DataType;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class RowCodecTest {

    @Test
    public void testCodec() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("card");
        col1.setType(ColumnType.kString);
        schema.add(col1);

        ColumnDesc col2 = new ColumnDesc();
        col2.setAddTsIndex(true);
        col2.setName("merchant");
        col2.setType(ColumnType.kString);
        schema.add(col2);

        ColumnDesc col3 = new ColumnDesc();
        col3.setAddTsIndex(false);
        col3.setName("amt");
        col3.setType(ColumnType.kDouble);
        schema.add(col3);

        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{"9527", "1234", 1.0}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(3, row.length);
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals("1234", row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    public String genString(char a, int length) {
        char[] value = new char[length];
        for (int i = 0; i < length; i++) {
            value[i] = a;
        }
        return new String(value);
    }

    @Test
    public void testStringCodec() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        for (int i = 0; i < 10; i++) {
            ColumnDesc col = new ColumnDesc();
            col.setAddTsIndex(true);
            col.setName("col" + i);
            col.setType(ColumnType.kString);
            schema.add(col);
        }
        ColumnDesc col = new ColumnDesc();
        col.setAddTsIndex(false);
        col.setName("amt");
        col.setType(ColumnType.kDouble);
        schema.add(col);
        String str_127 = genString('a', 127);
        String str_128 = genString('b', 128);
        String str_129 = genString('c', 129);
        String str_255 = genString('d', 255);
        String str_256 = genString('e', 256);
        String str_257 = genString('f', 257);
        String str_1000 = genString('g', 1000);
        String str_32766 = genString('h', 32766);
        String str_32767 = genString('i', 32767);
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{"abcd", str_127, str_128, str_129, str_255, str_256,
                    str_257, str_1000, str_32766, str_32767, 1.0}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(11, row.length);
            Assert.assertEquals("abcd", row[0]);
            Assert.assertEquals(str_127, row[1]);
            Assert.assertEquals(str_128, row[2]);
            Assert.assertEquals(str_129, row[3]);
            Assert.assertEquals(str_255, row[4]);
            Assert.assertEquals(str_256, row[5]);
            Assert.assertEquals(str_257, row[6]);
            Assert.assertEquals(str_1000, row[7]);
            Assert.assertEquals(str_32766, row[8]);
            Assert.assertEquals(str_32767, row[9]);
            Assert.assertEquals(1.0, row[10]);
        } catch (TabletException e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCodecMaxLength() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("card");
        col1.setType(ColumnType.kString);
        schema.add(col1);
        String str_32768 = genString('a', 32768);
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{str_32768}, schema);
            Object[] row = RowCodec.decode(buffer, schema);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testCodecWithTimestamp() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("ts");
        col1.setType(ColumnType.kTimestamp);
        schema.add(col1);
        long time = 1530772193000l;
        System.out.println(new Timestamp(time));
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{new DateTime(time)}, schema);
//            ByteBuffer buffer = RowCodec.encode(new Object[] {time}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] instanceof DateTime);
            Assert.assertEquals(time, ((DateTime) row[0]).getMillis());
            buffer = RowCodec.encode(new Object[]{null}, schema);
            buffer.rewind();
            row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] == null);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCodecWithShort() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("ts");
        col1.setType(ColumnType.kInt16);
        schema.add(col1);
        short i = 10;
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{i}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] instanceof Short);
            Assert.assertEquals(i, row[0]);
            buffer = RowCodec.encode(new Object[]{null}, schema);
            buffer.rewind();
            row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] == null);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCodecWithBool() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("ts");
        col1.setType(ColumnType.kBool);
        schema.add(col1);
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{true}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] instanceof Boolean);
            Assert.assertEquals(true, row[0]);
            buffer = RowCodec.encode(new Object[]{null}, schema);
            buffer.rewind();
            row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] == null);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCodecWithDate() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("ts");
        col1.setType(ColumnType.kDate);
        schema.add(col1);
        long time = 1530772193000l;
        Date target = new Date(time);
        DateTime jodaTime = new DateTime(time);
        System.out.println(target);
        System.out.println(jodaTime);
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{target}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] instanceof Date);
            Assert.assertEquals(target, (Date) row[0]);
            buffer = RowCodec.encode(new Object[]{null}, schema);
            buffer.rewind();
            row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(1, row.length);
            Assert.assertTrue(row[0] == null);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }


    @Test
    public void testDecodeWithArray() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("card");
        col1.setType(ColumnType.kString);
        schema.add(col1);

        ColumnDesc col2 = new ColumnDesc();
        col2.setAddTsIndex(true);
        col2.setName("merchant");
        col2.setType(ColumnType.kString);
        schema.add(col2);

        ColumnDesc col3 = new ColumnDesc();
        col3.setAddTsIndex(false);
        col3.setName("amt");
        col3.setType(ColumnType.kDouble);
        schema.add(col3);
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{"9527", "1234", 1.0}, schema);
            buffer.rewind();
            Object[] row = new Object[10];
            RowCodec.decode(buffer, schema, row, 7, 3);
            Assert.assertEquals("9527", row[7]);
            Assert.assertEquals("1234", row[8]);
            Assert.assertEquals(1.0, row[9]);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCodecPerf() {
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("card");
        col1.setType(ColumnType.kString);
        schema.add(col1);

        ColumnDesc col2 = new ColumnDesc();
        col2.setAddTsIndex(true);
        col2.setName("merchant");
        col2.setType(ColumnType.kDouble);
        schema.add(col2);

        ColumnDesc col3 = new ColumnDesc();
        col3.setAddTsIndex(false);
        col3.setName("amt");
        col3.setType(ColumnType.kInt64);
        schema.add(col3);

        ColumnDesc col4 = new ColumnDesc();
        col4.setAddTsIndex(false);
        col4.setName("amt1");
        col4.setType(ColumnType.kInt32);
        schema.add(col4);

        ColumnDesc col5 = new ColumnDesc();
        col5.setAddTsIndex(false);
        col5.setName("amt3");
        col5.setType(ColumnType.kFloat);
        schema.add(col5);


        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{"12345678910", 1.1d, 1000l, 1000, 2.1f}, schema);
            int i = 1000;
            while (i > 0) {
                buffer.rewind();
                Object[] row = RowCodec.decode(buffer, schema);
                i--;
            }
            i = 100000;
            Long consumed = System.nanoTime();
            while (i > 0) {
                buffer.rewind();
                Object[] row = RowCodec.decode(buffer, schema);
                i--;
            }
            consumed = System.nanoTime() - consumed;
            System.out.println(consumed);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testEmptyString() {
//        System.out.println("testEmptyString:");
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc col1 = new ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("empty");
        col1.setType(ColumnType.kString);
        schema.add(col1);

        ColumnDesc col2 = new ColumnDesc();
        col2.setAddTsIndex(true);
        col2.setType(ColumnType.kString);
        col2.setName("string");
        schema.add(col2);

        ColumnDesc col3 = new ColumnDesc();
        col3.setAddTsIndex(true);
        col3.setName("null");
        col3.setType(ColumnType.kInt32);
        schema.add(col3);

        ColumnDesc col4 = new ColumnDesc();
        col4.setAddTsIndex(true);
        col4.setName("empty" +
                "string is not string");
        col4.setType(ColumnType.kString);
        schema.add(col4);

        ColumnDesc col5 = new ColumnDesc();
        col5.setAddTsIndex(true);
        col5.setName("emptystring");
        col5.setType(ColumnType.kString);
        schema.add(col5);

        ColumnDesc col6 = new ColumnDesc();
        col6.setAddTsIndex(true);
        col6.setName("emptystring");
        col6.setType(ColumnType.kString);
        schema.add(col6);

        ColumnDesc col7 = new ColumnDesc();
        col7.setAddTsIndex(true);
        col7.setName("emptystring");
        col7.setType(ColumnType.kString);
        schema.add(col7);

        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{"", "I am a string", null, null, " ", "", ""}, schema);
            buffer.rewind();
            Object[] row = RowCodec.decode(buffer, schema);
            Assert.assertEquals(7, row.length);
            Assert.assertEquals("", row[0]);
            Assert.assertEquals("I am a string", row[1]);
            Assert.assertEquals(null, row[2]);
            Assert.assertEquals(null, row[3]);
            Assert.assertEquals(" ", row[4]);
            Assert.assertEquals("", row[5]);
            Assert.assertEquals("", row[6]);
            for (int i = 0; i < row.length; i++) {
                System.out.println("schema.get(i).getType() = " + schema.get(i).getType() + " ;  value = " + row[i]);
            }
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }


    }

    @Test
    public void testNull() {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            {
                ColumnDesc col1 = new ColumnDesc();
                col1.setName("col1");
                col1.setDataType(DataType.kInt16);
                schema.add(col1);
            }
            {
                ColumnDesc col2 = new ColumnDesc();
                col2.setName("col2");
                col2.setDataType(DataType.kBool);
                schema.add(col2);
            }
            {
                ColumnDesc col3 = new ColumnDesc();
                col3.setName("col3");
                col3.setDataType(DataType.kVarchar);
                schema.add(col3);
            }
            RowBuilder builder = new RowBuilder(schema);
            int size = builder.calTotalLength(1);
            ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
            buffer = builder.setBuffer(buffer, size);
            Assert.assertTrue(builder.appendNULL());
            Assert.assertTrue(builder.appendBool(false));
            Assert.assertTrue(builder.appendString("1"));

            RowView rowView = new RowView(schema, buffer, size);
            Assert.assertTrue(rowView.isNull(0));
            Assert.assertEquals(rowView.getBool(1), false);
            Assert.assertEquals(rowView.getString(2), "1");
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

}
