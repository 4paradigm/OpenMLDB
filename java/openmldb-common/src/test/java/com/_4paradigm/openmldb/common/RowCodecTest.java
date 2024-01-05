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

package com._4paradigm.openmldb.common;

import com._4paradigm.openmldb.common.codec.FlexibleRowBuilder;
import com._4paradigm.openmldb.common.codec.RowBuilder;
import com._4paradigm.openmldb.proto.Type.DataType;
import com._4paradigm.openmldb.proto.Common.ColumnDesc;
import com._4paradigm.openmldb.common.codec.RowView;
import com._4paradigm.openmldb.common.codec.ClassicRowBuilder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.sql.Date;
import java.util.*;

public class RowCodecTest {

    private List<DataType> typeList = new ArrayList<>(Arrays.asList(
            DataType.kBool,
            DataType.kSmallInt,
            DataType.kInt,
            DataType.kBigInt,
            DataType.kFloat,
            DataType.kDouble,
            DataType.kDate,
            DataType.kTimestamp,
            DataType.kString,
            DataType.kVarchar)
    );

    @DataProvider(name = "builder")
    Object[] getData() {
        return new Object[] {"classic", "flexible"};
    }

    @Test(dataProvider = "builder")
    public void testNull(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            schema.add(ColumnDesc.newBuilder().setName("col1").setDataType(DataType.kSmallInt).build());
            schema.add(ColumnDesc.newBuilder().setName("col2").setDataType(DataType.kBool).build());
            schema.add(ColumnDesc.newBuilder().setName("col3").setDataType(DataType.kVarchar).build());
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(9);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }
            Assert.assertTrue(builder.appendNULL());
            Assert.assertTrue(builder.appendBool(false));
            Assert.assertTrue(builder.appendString("123456789"));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            Assert.assertTrue(rowView.isNull(0));
            Assert.assertFalse(rowView.isNull(1));
            Assert.assertEquals(rowView.getBool(1), new Boolean(false));
            Assert.assertEquals(rowView.getString(2), "123456789");

            RowView rowView2 = new RowView(schema);
            Object value = rowView2.getValue(buffer, 2, DataType.kVarchar);
            Assert.assertEquals((String) value, "123456789");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testValueNull(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            schema.add(ColumnDesc.newBuilder().setName("col1").setDataType(DataType.kTimestamp).build());
            schema.add(ColumnDesc.newBuilder().setName("col2").setDataType(DataType.kDate).build());
            schema.add(ColumnDesc.newBuilder().setName("col3").setDataType(DataType.kVarchar).build());
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(0);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }
            Assert.assertTrue(builder.appendTimestamp(null));
            Assert.assertTrue(builder.appendDate(null));
            Assert.assertTrue(builder.appendString(null));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            Assert.assertTrue(rowView.isNull(0));
            Assert.assertTrue(rowView.isNull(1));
            Assert.assertTrue(rowView.isNull(2));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testNormal(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            schema.add(ColumnDesc.newBuilder().setName("col1").setDataType(DataType.kInt).build());
            schema.add(ColumnDesc.newBuilder().setName("col2").setDataType(DataType.kSmallInt).build());
            schema.add(ColumnDesc.newBuilder().setName("col3").setDataType(DataType.kFloat).build());
            schema.add(ColumnDesc.newBuilder().setName("col4").setDataType(DataType.kDouble).build());
            schema.add(ColumnDesc.newBuilder().setName("col5").setDataType(DataType.kBigInt).build());
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(1);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                buffer = cBuilder.setBuffer(buffer, size);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }
            Assert.assertTrue(builder.appendInt(1));
            Assert.assertTrue(builder.appendSmallInt((short) 2));
            Assert.assertTrue(builder.appendFloat(3.1f));
            Assert.assertTrue(builder.appendDouble(4.1));
            Assert.assertTrue(builder.appendBigInt(5));
            builder.build();

            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            Assert.assertEquals(rowView.getInt(0), new Integer(1));
            Assert.assertEquals(rowView.getSmallInt(1), new Short((short) 2));
            Assert.assertEquals(rowView.getFloat(2), 3.1f);
            Assert.assertEquals(rowView.getDouble(3), 4.1);
            Assert.assertEquals(rowView.getBigInt(4), new Long(5));
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testEncode(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 10; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + i);
                if (i % 3 == 0) {
                    col.setDataType(DataType.kSmallInt);
                } else if (i % 3 == 1) {
                    col.setDataType(DataType.kDouble);
                } else {
                    col.setDataType(DataType.kVarchar);
                }
                schema.add(col.build());
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(30);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }

            for (int i = 0; i < 10; i++) {
                if (i % 3 == 0) {
                    Assert.assertTrue(builder.appendSmallInt((short) i));
                } else if (i % 3 == 1) {
                    Assert.assertTrue(builder.appendDouble(2.3));
                } else {
                    String s = String.join("", Collections.nCopies(10, String.valueOf(i)));
                    Assert.assertTrue(builder.appendString(s));
                }
            }
            Assert.assertFalse(builder.appendSmallInt((short) 1));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();

            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (int i = 0; i < 10; i++) {
                if (i % 3 == 0) {
                    Assert.assertEquals(rowView.getSmallInt(i), new Short((short) i));
                } else if (i % 3 == 1) {
                    Assert.assertEquals(rowView.getDouble(i), 2.3);
                } else {
                    String s = String.join("", Collections.nCopies(10, String.valueOf(i)));
                    Assert.assertEquals(rowView.getString(i), s);
                }
            }
            try {
                rowView.getDouble(10);
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testAppendNull(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 20; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + i);
                if (i % 3 == 0) {
                    col.setDataType(DataType.kSmallInt);
                } else if (i % 3 == 1) {
                    col.setDataType(DataType.kDouble);
                } else {
                    col.setDataType(DataType.kVarchar);
                }
                schema.add(col.build());
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(30);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                buffer = cBuilder.setBuffer(buffer, size);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }

            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(builder.appendNULL());
                    continue;
                }
                if (i % 3 == 0) {
                    Assert.assertTrue(builder.appendSmallInt((short) i));
                } else if (i % 3 == 1) {
                    Assert.assertTrue(builder.appendDouble(2.3));
                } else {
                    String s = String.join("", Collections.nCopies(10, String.valueOf(i % 10)));
                    Assert.assertTrue(builder.appendString(s));
                }
            }
            Assert.assertFalse(builder.appendSmallInt((short) 1));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();

            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(rowView.isNull(i));
                    if (i % 3 == 0) {
                        Assert.assertEquals(rowView.getSmallInt(i), null);
                    } else if (i % 3 == 1) {
                        Assert.assertEquals(rowView.getDouble(i), null);
                    } else {
                        Assert.assertEquals(rowView.getString(i), null);
                    }
                    continue;
                }
                if (i % 3 == 0) {
                    Assert.assertEquals(rowView.getSmallInt(i), new Short((short) i));
                } else if (i % 3 == 1) {
                    Assert.assertEquals(rowView.getDouble(i), 2.3);
                } else {
                    String s = String.join("", Collections.nCopies(10, String.valueOf(i % 10)));
                    Assert.assertEquals(rowView.getString(i), s);
                }
            }
            try {
                rowView.getDouble(20);
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testAppendNullAndEmpty(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 20; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + i);
                if (i % 2 == 0) {
                    col.setDataType(DataType.kSmallInt);
                } else {
                    col.setDataType(DataType.kVarchar);
                }
                schema.add(col.build());
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(30);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }


            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    if (i % 3 == 0) {
                        Assert.assertTrue(builder.appendNULL());
                    } else {
                        Assert.assertTrue(builder.appendSmallInt((short) i));
                    }
                } else {
                    if (i % 3 == 0) {
                        Assert.assertTrue(builder.appendNULL());
                    } else if (i % 3 == 1) {
                        Assert.assertTrue(builder.appendString(""));
                    } else {
                        String s = String.join("", Collections.nCopies(10, String.valueOf(i % 10)));
                        Assert.assertTrue(builder.appendString(s));
                    }
                }
            }
            Assert.assertFalse(builder.appendSmallInt((short) 1));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (int i = 0; i < 20; i++) {
                if (i % 2 == 0) {
                    if (i % 3 == 0) {
                        Assert.assertTrue(rowView.isNull(i));
                        Assert.assertEquals(rowView.getSmallInt(i), null);
                    } else {
                        Assert.assertEquals(rowView.getSmallInt(i), new Short((short) i));
                    }
                } else {
                    if (i % 3 == 0) {
                        Assert.assertTrue(rowView.isNull(i));
                        Assert.assertEquals(rowView.getString(i), null);
                    } else if (i % 3 == 1) {
                        Assert.assertEquals(rowView.getString(i), "");
                    } else {
                        String s = String.join("", Collections.nCopies(10, String.valueOf(i % 10)));
                        Assert.assertEquals(rowView.getString(i), s);
                    }
                }
            }
            try {
                rowView.getDouble(20);
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testManyCol(String builderName) {
        int[] arr = {10, 20, 50, 100, 1000, 10000, 100000};
        try {
            for (int colNum : arr) {
                List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
                for (int i = 0; i < colNum; i++) {
                    {
                        ColumnDesc.Builder col = ColumnDesc.newBuilder();
                        col.setName("col" + i + 1);
                        col.setDataType(DataType.kVarchar);
                        schema.add(col.build());
                    }
                    {
                        ColumnDesc.Builder col = ColumnDesc.newBuilder();
                        col.setName("col" + i + 2);
                        col.setDataType(DataType.kBigInt);
                        schema.add(col.build());
                    }
                    {
                        ColumnDesc.Builder col = ColumnDesc.newBuilder();
                        col.setName("col" + i + 3);
                        col.setDataType(DataType.kDouble);
                        schema.add(col.build());
                    }
                }
                RowBuilder builder;
                if (builderName.equals("classic")) {
                    ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                    int size = cBuilder.calTotalLength(10 * colNum);
                    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                    cBuilder.setBuffer(buffer, size);
                    builder = cBuilder;
                } else {
                    builder = new FlexibleRowBuilder(schema);
                }

                long base = 1000000000l;
                long ts = 1576811755000l;
                for (int idx = 0; idx < colNum; idx++) {
                    Assert.assertTrue(builder.appendString(String.valueOf(base + idx)));
                    Assert.assertTrue(builder.appendBigInt(ts + idx));
                    Assert.assertTrue(builder.appendDouble(1.3));
                }
                Assert.assertTrue(builder.build());
                ByteBuffer buffer = builder.getValue();

                RowView rowView = new RowView(schema, buffer, buffer.capacity());
                for (int idx = 0; idx < colNum; idx++) {
                    String s = String.valueOf(base + idx);
                    Assert.assertEquals(rowView.getString(3 * idx), s);
                    Assert.assertEquals(rowView.getBigInt(3 * idx + 1), new Long(ts + idx));
                    Assert.assertEquals(rowView.getDouble(3 * idx + 2), 1.3);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testNotAppendString(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 10; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + i);
                col.setDataType(DataType.kVarchar);
                schema.add(col.build());
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(100);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }

            for (int i = 0; i < 7; i++) {
                if (i == 0) {
                    Assert.assertTrue(builder.appendNULL());
                } else {
                    String s = String.join("", Collections.nCopies(10, String.valueOf(i % 10)));
                    Assert.assertTrue(builder.appendString(s));
                }
            }
            Assert.assertFalse(builder.appendSmallInt((short) 1));
            if (builderName.equals("classic")) {
                Assert.assertTrue(builder.build());
                ByteBuffer buffer = builder.getValue();
                RowView rowView = new RowView(schema, buffer, buffer.capacity());
                for (int i = 0; i < 10; i++) {
                    if (i == 0) {
                        Assert.assertTrue(rowView.isNull(i));
                    } else if (i < 7) {
                        String s = String.join("", Collections.nCopies(10, String.valueOf(i % 10)));
                        Assert.assertEquals(rowView.getString(i), s);
                    } else {
                        Assert.assertTrue(rowView.isNull(i));
                    }
                }
            } else {
                Assert.assertFalse(builder.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test(dataProvider = "builder")
    public void testEncodeRow(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 10; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + String.valueOf(i));
                if (i % 2 == 0) {
                    col.setDataType(DataType.kBigInt);
                } else {
                    col.setDataType(DataType.kVarchar);
                }
                schema.add(col.build());
            }
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    row.add(Long.valueOf(i));
                } else {
                    row.add(new String("aaa") + String.valueOf(i));
                }
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(row);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }

            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(builder.appendBigInt((Long)row.get(i)));
                } else {
                    Assert.assertTrue(builder.appendString((String)row.get(i)));
                }
            }
            Assert.assertFalse(builder.appendSmallInt((short) 1));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();

            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(rowView.getBigInt(i) == i);
                } else {
                    Assert.assertEquals(new String("aaa") + String.valueOf(i), rowView.getString(i));
                }
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test(dataProvider = "builder")
    public void testAppendNull2(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 2; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + i);
                col.setDataType(DataType.kVarchar);
                schema.add(col.build());
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(10);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                buffer = cBuilder.setBuffer(buffer, size);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }

            for (int i = 0; i < 2; i++) {
                if (i == 0) {
                    Assert.assertTrue(builder.appendNULL());
                    continue;
                }
                String s = String.join("", Collections.nCopies(10, String.valueOf(i)));
                Assert.assertTrue(builder.appendString(s));
            }
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();

            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (int i = 0; i < 2; i++) {
                if (i == 0) {
                    Assert.assertTrue(rowView.isNull(i));
                    Assert.assertEquals(rowView.getString(i), null);
                    continue;
                }
                String s = String.join("", Collections.nCopies(10, String.valueOf(i)));
                Assert.assertEquals(rowView.getString(i), s);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @DataProvider(name = "columnNum")
    Object[] getColNum() {
        return new Object[] {1, 3, 5, 10, 20, 30, 50, 100, 200, 300, 500, 1000, 2000, 3000, 5000, 10000, 20000, 100000};
    }

    public String genRandomString(int len) {
        Random r = new Random();
        char[] arr = new char[len];
        for (int i = 0; i < len; i++) {
            arr[i] = (char)(32 + (int)(94 * r.nextDouble()));
        }
        return new String(arr);
    }

    Object[] genData(List<ColumnDesc> schema) {
        Random r = new Random();
        Object[] data = new Object[schema.size()];
        for (int idx = 0; idx < schema.size(); idx++) {
            if (r.nextInt() % 5 == 0) {
                data[idx] = null;
                continue;
            }
            DataType type = schema.get(idx).getDataType();
            if (type == DataType.kBool) {
                data[idx] = r.nextInt() % 2 == 0 ? true : false;
            } else if (type == DataType.kSmallInt) {
                data[idx] = (short)r.nextInt(10000);
            } else if (type == DataType.kInt) {
                data[idx] = r.nextInt(10000);
            } else if (type == DataType.kBigInt) {
                data[idx] = (long)r.nextInt(10000);
            } else if (type == DataType.kFloat) {
                data[idx] = r.nextFloat();
            } else if (type == DataType.kDouble) {
                data[idx] = r.nextDouble();
            } else if (type == DataType.kDate) {
                data[idx] = new java.sql.Date(r.nextInt(8000), r.nextInt(11), r.nextInt(25));
            } else if (type == DataType.kTimestamp) {
                data[idx] = new Timestamp(System.currentTimeMillis());
            } else if (type == DataType.kVarchar || type == DataType.kString) {
                data[idx] = r.nextInt() % 3 == 0 ? "" : genRandomString(r.nextInt(10));
            }
        }
        return data;
    }

    void setData(RowBuilder builder, int idx, DataType type, Object obj) {
        if (obj == null) {
            builder.setNULL(idx);
            return;
        }
        if (type == DataType.kBool) {
            builder.setBool(idx, (boolean)obj);
        } else if (type == DataType.kSmallInt) {
            builder.setSmallInt(idx, (short)obj);
        } else if (type == DataType.kInt) {
            builder.setInt(idx, (int)obj);
        } else if (type == DataType.kBigInt) {
            builder.setBigInt(idx, (long)obj);
        } else if (type == DataType.kFloat) {
            builder.setFloat(idx, (float)obj);
        } else if (type == DataType.kDouble) {
            builder.setDouble(idx, (double)obj);
        } else if (type == DataType.kDate) {
            builder.setDate(idx, (java.sql.Date)obj);
        } else if (type == DataType.kTimestamp) {
            builder.setTimestamp(idx, (Timestamp) obj);
        } else if (type == DataType.kString || type == DataType.kVarchar) {
            builder.setString(idx, (String)obj);
        }
    }

    void checkData(RowView rowView, int idx, DataType type, Object exp) throws Exception {
        if (exp == null) {
            Assert.assertTrue(rowView.isNull(idx));
            return;
        } else {
            Assert.assertFalse(rowView.isNull(idx));
        }
        if (type == DataType.kBool) {
            Assert.assertEquals(rowView.getBool(idx), exp);
        } else if (type == DataType.kSmallInt) {
            Assert.assertEquals(rowView.getSmallInt(idx), exp);
        } else if (type == DataType.kInt) {
            Assert.assertEquals(rowView.getInt(idx), exp);
        } else if (type == DataType.kBigInt) {
            Assert.assertEquals(rowView.getBigInt(idx), exp);
        } else if (type == DataType.kFloat) {
            Assert.assertEquals(rowView.getFloat(idx), exp);
        } else if (type == DataType.kDouble) {
            Assert.assertEquals(rowView.getDouble(idx), exp);
        } else if (type == DataType.kDate) {
            Assert.assertEquals(rowView.getDate(idx), exp);
        } else if (type == DataType.kTimestamp) {
            Assert.assertEquals(rowView.getTimestamp(idx), exp);
        } else if (type == DataType.kString || type == DataType.kVarchar) {
            Assert.assertEquals(rowView.getString(idx), (String)exp);
        } else {
            Assert.fail();
        }
    }

    @Test(dataProvider = "columnNum")
    public void testDisorderPutBase(int columnNum) {
        Random r = new Random();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        List<Integer> idx = new ArrayList<>();
        for (int i = 0; i < columnNum; i++) {
            ColumnDesc.Builder col = ColumnDesc.newBuilder();
            col.setName("col" + i);
            while (true) {
                DataType type = typeList.get(r.nextInt(typeList.size()));
                if (type != DataType.kString && type != DataType.kVarchar) {
                    col.setDataType(type);
                    break;
                }
            }
            schema.add(col.build());
            idx.add(i);
        }
        Collections.shuffle(idx);
        Object[] data = genData(schema);
        try {
            FlexibleRowBuilder builder = new FlexibleRowBuilder(schema);
            for (Integer i : idx) {
                setData(builder, i, schema.get(i).getDataType(), data[i]);
            }
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (Integer i : idx) {
                checkData(rowView, i, schema.get(i).getDataType(), data[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(dataProvider = "columnNum")
    public void testDisorderPut(int columnNum) {
        Random r = new Random();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        List<Integer> idx = new ArrayList<>();
        for (int i = 0; i < columnNum; i++) {
            ColumnDesc.Builder col = ColumnDesc.newBuilder();
            col.setName("col" + i);
            DataType type = typeList.get(r.nextInt(typeList.size()));
            col.setDataType(type);
            schema.add(col.build());
            idx.add(i);
        }
        Collections.shuffle(idx);
        Object[] data = genData(schema);
        try {
            FlexibleRowBuilder builder = new FlexibleRowBuilder(schema);
            for (Integer i : idx) {
                setData(builder, i, schema.get(i).getDataType(), data[i]);
            }
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (Integer i : idx) {
                try {
                    checkData(rowView, i, schema.get(i).getDataType(), data[i]);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(dataProvider = "columnNum")
    public void testDisorderStringOnly(int columnNum) {
        Random r = new Random();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        List<Integer> idx = new ArrayList<>();
        String[] data = new String[columnNum];
        for (int i = 0; i < columnNum; i++) {
            ColumnDesc.Builder col = ColumnDesc.newBuilder();
            col.setName("col" + i);
            col.setDataType(DataType.kString);
            schema.add(col.build());
            idx.add(i);
            data[i] = genRandomString(r.nextInt(1000));
        }
        Collections.shuffle(idx);

        try {
            FlexibleRowBuilder builder = new FlexibleRowBuilder(schema);
            for (Integer i : idx) {
                setData(builder, i, schema.get(i).getDataType(), data[i]);
            }
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (Integer i : idx) {
                try {
                    checkData(rowView, i, schema.get(i).getDataType(), data[i]);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test(dataProvider = "builder")
    public void testSetMultiTimes(String builderName) {
        try {
            List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
            for (int i = 0; i < 10; i++) {
                ColumnDesc.Builder col = ColumnDesc.newBuilder();
                col.setName("col" + String.valueOf(i));
                if (i % 2 == 0) {
                    col.setDataType(DataType.kBigInt);
                } else {
                    col.setDataType(DataType.kVarchar);
                }
                schema.add(col.build());
            }
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    row.add(Long.valueOf(i));
                } else {
                    row.add(new String("aaa") + String.valueOf(i));
                }
            }
            RowBuilder builder;
            if (builderName.equals("classic")) {
                ClassicRowBuilder cBuilder = new ClassicRowBuilder(schema);
                int size = cBuilder.calTotalLength(row);
                ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
                cBuilder.setBuffer(buffer, size);
                builder = cBuilder;
            } else {
                builder = new FlexibleRowBuilder(schema);
            }

            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(builder.setBigInt(i, (Long)row.get(i)));
                    if (builder instanceof ClassicRowBuilder) {
                        Assert.assertFalse(builder.setBigInt(i, (Long)row.get(i)));
                    } else {
                        Assert.assertTrue(builder.setBigInt(i, (Long)row.get(i)));
                    }
                } else {
                    Assert.assertTrue(builder.setString(i, (String)row.get(i)));
                    Assert.assertFalse(builder.setString(i, (String)row.get(i)));
                }
            }
            Assert.assertFalse(builder.appendSmallInt((short) 1));
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();

            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (int i = 0; i < 10; i++) {
                if (i % 2 == 0) {
                    Assert.assertTrue(rowView.getBigInt(i) == i);
                } else {
                    Assert.assertEquals(new String("aaa") + String.valueOf(i), rowView.getString(i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSpecialCase() {
        List<DataType> types = new ArrayList<>(Arrays.asList(
                /* 0  */ DataType.kTimestamp,
                /* 1  */ DataType.kBool,
                /* 2  */ DataType.kSmallInt,
                /* 3  */ DataType.kInt,
                /* 4  */ DataType.kVarchar,
                /* 5  */ DataType.kBool,
                /* 6  */ DataType.kTimestamp,
                /* 7  */ DataType.kFloat,
                /* 8  */ DataType.kInt,
                /* 9  */ DataType.kInt,
                /* 10 */ DataType.kString,
                /* 11 */ DataType.kVarchar,
                /* 12 */ DataType.kInt,
                /* 13 */ DataType.kTimestamp,
                /* 14 */ DataType.kBool,
                /* 15 */ DataType.kInt,
                /* 16 */ DataType.kInt,
                /* 17 */ DataType.kDouble,
                /* 18 */ DataType.kInt,
                /* 19 */ DataType.kDate,
                /* 20 */ DataType.kDate,
                /* 21 */ DataType.kSmallInt,
                /* 22 */ DataType.kString,
                /* 23 */ DataType.kDouble,
                /* 24 */ DataType.kBigInt,
                /* 25 */ DataType.kBigInt,
                /* 26 */ DataType.kVarchar,
                /* 27 */ DataType.kSmallInt,
                /* 28 */ DataType.kBigInt,
                /* 29 */ DataType.kDate,
                /* 30 */ DataType.kDate,
                /* 31 */ DataType.kDouble,
                /* 32 */ DataType.kString,
                /* 33 */ DataType.kFloat,
                /* 34 */ DataType.kBool,
                /* 35 */ DataType.kTimestamp,
                /* 36 */ DataType.kBool,
                /* 37 */ DataType.kFloat,
                /* 38 */ DataType.kVarchar,
                /* 39 */ DataType.kInt,
                /* 40 */ DataType.kBool,
                /* 41 */ DataType.kFloat,
                /* 42 */ DataType.kInt,
                /* 43 */ DataType.kDate,
                /* 44 */ DataType.kTimestamp,
                /* 45 */ DataType.kBigInt,
                /* 46 */ DataType.kSmallInt,
                /* 47 */ DataType.kInt,
                /* 48 */ DataType.kVarchar,
                /* 49 */ DataType.kBool
        ));
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        List<Integer> idx = new ArrayList<>(Arrays.asList(
                36, 3, 31, 8, 27, 10, 49, 44, 23, 20, 40, 28, 25, 19, 29, 24, 11, 21, 13, 30, 46, 26, 41, 1, 17,
                5, 45, 37, 43, 0, 38, 33, 22, 35, 39, 12, 9, 16, 48, 6, 7, 34, 47, 42, 4, 15, 14, 2, 32, 18
        ));
        for (int i = 0; i < types.size(); i++) {
            ColumnDesc.Builder col = ColumnDesc.newBuilder();
            col.setName("col" + i);
            DataType type = types.get(i);
            col.setDataType(type);
            schema.add(col.build());
        }
        Object[] data = {
                /* 0  */ new Timestamp(System.currentTimeMillis()),
                /* 1  */ true,
                /* 2  */ null,
                /* 3  */ null,
                /* 4  */ "val10417",
                /* 5  */ false,
                /* 6  */ new Timestamp(System.currentTimeMillis()),
                /* 7  */ 0.759f,
                /* 8  */ 9053,
                /* 9  */ null,
                /* 10 */ "val10564",
                /* 11 */ "",
                /* 12 */ 5870,
                /* 13 */ new Timestamp(System.currentTimeMillis()),
                /* 14 */ false,
                /* 15 */ 3155,
                /* 16 */ 3022,
                /* 17 */ 0.78d,
                /* 18 */ 8100,
                /* 19 */ null,
                /* 20 */ new Date(2542, 3, 13),
                /* 21 */ (short)8712,
                /* 22 */ "val10187",
                /* 23 */ 0.39d,
                /* 24 */ (long)1941,
                /* 25 */ 357l,
                /* 26 */ "val10139",
                /* 27 */ (short)376,
                /* 28 */ null,
                /* 29 */ new Date(3041, 3, 8),
                /* 30 */ new Date(2782, 6, 22),
                /* 31 */ 0.22d,
                /* 32 */ "val10117",
                /* 33 */ 0.19f,
                /* 34 */ false,
                /* 35 */ new Timestamp(System.currentTimeMillis()),
                /* 36 */ false,
                /* 37 */ 0.49f,
                /* 38 */ "",
                /* 39 */ 1117,
                /* 40 */ false,
                /* 41 */ 0.61f,
                /* 42 */ 489,
                /* 43 */ new Date(5282, 8, 8),
                /* 44 */ null,
                /* 45 */ null,
                /* 46 */ null,
                /* 47 */ null,
                /* 48 */ "val10738",
                /* 49 */ false
        };
        try {
            FlexibleRowBuilder builder = new FlexibleRowBuilder(schema);
            for (Integer i : idx) {
                setData(builder, i, schema.get(i).getDataType(), data[i]);
            }
            Assert.assertTrue(builder.build());
            ByteBuffer buffer = builder.getValue();
            RowView rowView = new RowView(schema, buffer, buffer.capacity());
            for (Integer i : idx) {
                try {
                    checkData(rowView, i, schema.get(i).getDataType(), data[i]);
                } catch (Exception e) {
                    Assert.fail();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}

