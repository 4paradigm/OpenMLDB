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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RowCodecTest {

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

}

