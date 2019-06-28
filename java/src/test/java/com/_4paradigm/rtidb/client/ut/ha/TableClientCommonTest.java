package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.impl.TableClientCommon;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableClientCommonTest {

    @Test
    public void testComputePidByKey() {
        Assert.assertEquals(TableClientCommon.computePidByKey("testkey", 8), 1);
        Assert.assertEquals(TableClientCommon.computePidByKey("xxxxx", 8), 7);
        Assert.assertEquals(TableClientCommon.computePidByKey("12345", 8), 7);
        Assert.assertEquals(TableClientCommon.computePidByKey("12345", 16), 15);
        Assert.assertEquals(TableClientCommon.computePidByKey("12345", 5), 4);
        Assert.assertEquals(TableClientCommon.computePidByKey("测试数据0", 8), 4);
        Assert.assertEquals(TableClientCommon.computePidByKey("测试数据1", 8), 7);
        Assert.assertEquals(TableClientCommon.computePidByKey("测试数据2", 8), 5);
        Assert.assertEquals(TableClientCommon.computePidByKey("0", 8), 3);
        Assert.assertEquals(TableClientCommon.computePidByKey("1", 8), 5);
        Assert.assertEquals(TableClientCommon.computePidByKey("2", 8), 0);
        Assert.assertEquals(TableClientCommon.computePidByKey("3", 8), 4);
    }

    @Test
    public void testGetCombinedKey() {
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card"}, false), "card");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{""}, true), "!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null}, true), "!N@U#L$L%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", "mcc"}, false), "card|mcc");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", "mcc", "1122"}, false), "card|mcc|1122");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card"}, false), "card");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", "|"}, false), "card||");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", "mcc"}, true), "card|mcc");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", ""}, true), "card|!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", null}, true), "card|!N@U#L$L%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", null}, false), "card|!N@U#L$L%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"card", ""}, false), "card|!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"", ""}, true), "!@#$%|!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, ""}, true), "!N@U#L$L%|!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, null}, true), "!N@U#L$L%|!N@U#L$L%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, null, ""}, true), "!N@U#L$L%|!N@U#L$L%|!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, null, "aaa"}, false), "!N@U#L$L%|!N@U#L$L%|aaa");
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, "", "bbb"}, false), "!N@U#L$L%|!@#$%|bbb");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{"", ""}, false), "!@#$%|!@#$%");
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, ""}, false), "!N@U#L$L%|!@#$%");
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, null}, false), "!N@U#L$L%|!@#$%");
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(new Object[]{null, null, ""}, false), "!N@U#L$L%|!N@U#L$L%|!@#$%");
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testGetCombinedKeyMap() {
        List<String> indexList = new ArrayList<String>() {{add("card"); add("mcc");}};
        Map<String, Object> keyMap = new HashMap<String, Object>();
        keyMap.put("card", "card1");
        keyMap.put("mcc", "mcc1");
        keyMap.put("amt", 1.5);
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, false), "card1|mcc1");
            keyMap.put("mcc", "");
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, true), "card1|!@#$%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, false), "card1|!@#$%");
            keyMap.put("mcc", null);
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, true), "card1|!N@U#L$L%");
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, false), "card1|!N@U#L$L%");
            keyMap.put("card", "");
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, true), "!@#$%|!N@U#L$L%");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, false), "!@#$%|!N@U#L$L%");
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        keyMap.remove("mcc");
        try {
            Assert.assertEquals(TableClientCommon.getCombinedKey(keyMap, indexList, false), "card1|!N@U#L$L%");
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testFillPartitionTabletDimension() {

        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(1000).setType("timestamp").build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card1").addColName("card").addTsName("ts1").addTsName("ts2").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addColName("mcc").addTsName("ts2").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName("test123").setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();

        TableHandler th = new TableHandler(table);
        th.setPartitions( new PartitionHandler[] {new PartitionHandler(), new PartitionHandler(), new PartitionHandler()});
        try {
            Object[] row = new Object[] {"card1", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            Map<Integer, List<Tablet.Dimension>> map = TableClientCommon.fillPartitionTabletDimension(row, th, false);
            Assert.assertEquals(map.size(), 1);
            Assert.assertEquals(map.get(0).size(), 2);
            Assert.assertEquals(map.get(0).get(0).getKey(), "card1");
            Assert.assertEquals(map.get(0).get(0).getIdx(), 0);
            Assert.assertEquals(map.get(0).get(1).getKey(), "mcc1");

            Object[] row1 = new Object[] {"", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            map = TableClientCommon.fillPartitionTabletDimension(row1, th, true);
            Assert.assertEquals(map.size(), 1);
            Assert.assertEquals(map.get(0).size(), 2);
            Assert.assertEquals(map.get(0).get(0).getKey(), "!@#$%");
            Assert.assertEquals(map.get(0).get(0).getIdx(), 0);
            Assert.assertEquals(map.get(0).get(1).getKey(), "mcc1");

            Object[] row2 = new Object[] {null, "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            map = TableClientCommon.fillPartitionTabletDimension(row2, th, true);
            Assert.assertEquals(map.size(), 2);
            Assert.assertEquals(map.get(0).size(), 1);
            Assert.assertEquals(map.get(0).get(0).getKey(), "mcc1");
            Assert.assertEquals(map.get(0).get(0).getIdx(), 1);
            Assert.assertEquals(map.get(1).get(0).getKey(), "!N@U#L$L%");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

        try {
            Object[] row1 = new Object[] {"", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            Map<Integer, List<Tablet.Dimension>> map = TableClientCommon.fillPartitionTabletDimension(row1, th, false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row1 = new Object[] {null, "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            Map<Integer, List<Tablet.Dimension>> map = TableClientCommon.fillPartitionTabletDimension(row1, th, false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testFillTabletDimension() {
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(1000).setType("timestamp").build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card1").addColName("card").addTsName("ts1").addTsName("ts2").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addColName("mcc").addTsName("ts2").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName("test123").setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();

        TableHandler th = new TableHandler(table);
        th.setPartitions( new PartitionHandler[] {new PartitionHandler(), new PartitionHandler(), new PartitionHandler()});

        try {
            Object[] row = new Object[] {"card1", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            List<Tablet.Dimension> list = TableClientCommon.fillTabletDimension(row, th, false);
            Assert.assertEquals(list.size(), 2);
            Assert.assertEquals(list.get(0).getKey(), "card1");
            Assert.assertEquals(list.get(1).getKey(), "mcc1");

            Object[] row1 = new Object[] {"", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            list = TableClientCommon.fillTabletDimension(row1, th, true);
            Assert.assertEquals(list.size(), 2);
            Assert.assertEquals(list.get(0).getKey(), "!@#$%");
            Assert.assertEquals(list.get(1).getKey(), "mcc1");

            Object[] row2 = new Object[] {null, "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            list = TableClientCommon.fillTabletDimension(row2, th, true);
            Assert.assertEquals(list.size(), 2);
            Assert.assertEquals(list.get(0).getKey(), "!N@U#L$L%");
            Assert.assertEquals(list.get(1).getKey(), "mcc1");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            Object[] row1 = new Object[] {"", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            List<Tablet.Dimension> list = TableClientCommon.fillTabletDimension(row1, th, false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row1 = new Object[] {null, "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2011-05-09 11:49:45")};
            List<Tablet.Dimension> list = TableClientCommon.fillTabletDimension(row1, th, false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testParseArrayInput() {
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(1000).setType("timestamp").build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card1").addColName("card").addTsName("ts1").addTsName("ts2").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addColName("mcc").addTsName("ts2").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName("test123").setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();

        TableHandler th = new TableHandler(table);
        th.setPartitions( new PartitionHandler[] {new PartitionHandler(), new PartitionHandler(), new PartitionHandler()});
        try {
            Timestamp ts = Timestamp.valueOf("2019-05-30 19:50:43");
            Object[] row1 = new Object[] {"card1", "mcc1", 1.5d, 1122l, 1234l, ts};
            List<Tablet.TSDimension> list = TableClientCommon.parseArrayInput(row1, th);
            Assert.assertEquals(list.size(), 2);
            Assert.assertEquals(list.get(0).getTs(), 1234);
            Assert.assertEquals(list.get(1).getTs(), ts.getTime());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }

        try {
            Object[] row1 = new Object[] {"card1", "mcc1", 1.5d, 1234l, Timestamp.valueOf("2019-05-30 19:50:43")};
            List<Tablet.TSDimension> list = TableClientCommon.parseArrayInput(row1, th);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }


        try {
            Common.ColumnDesc col6 = Common.ColumnDesc.newBuilder().setName("ts1").setType("int64").build();
            Common.ColumnDesc col7 = Common.ColumnDesc.newBuilder().setName("ts2").setType("timestamp").build();
            NS.TableInfo table1 = NS.TableInfo.newBuilder()
                    .setName("test123").setTtl(14400)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                    .addColumnDescV1(col6).addColumnDescV1(col7)
                    .build();
            th = new TableHandler(table1);
            th.setPartitions( new PartitionHandler[] {new PartitionHandler(), new PartitionHandler(), new PartitionHandler()});
            Object[] row1 = new Object[] {"card1", "mcc1", 1.5d, 1122l, 1234l, Timestamp.valueOf("2019-05-30 19:50:43")};
            List<Tablet.TSDimension> list = TableClientCommon.parseArrayInput(row1, th);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testParseMapInput() {
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(1000).setType("timestamp").build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card1").addColName("card").addTsName("ts1").addTsName("ts2").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addColName("mcc").addTsName("ts2").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName("test123").setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();

        TableHandler th = new TableHandler(table);
        th.setPartitions( new PartitionHandler[] {new PartitionHandler(), new PartitionHandler(), new PartitionHandler()});
        try {
            Timestamp ts = Timestamp.valueOf("2019-05-30 19:50:43");
            Map<String, Object> keyMap = new HashMap<String, Object>();
            keyMap.put("card", "card1");
            keyMap.put("mcc", "mcc1");
            keyMap.put("amt", 1.5d);
            keyMap.put("col1", 1234);
            keyMap.put("ts1", 1122l);
            keyMap.put("ts2", ts;
            Object[] arrayRow = new Object[th.getSchema().size()];
            List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
            TableClientCommon.parseMapInput(keyMap, th, arrayRow, tsDimensions);
            Assert.assertEquals(arrayRow[0], "card1");
            Assert.assertEquals(arrayRow[1], "mcc1");
            Assert.assertEquals(arrayRow[2], 1.5d);
            Assert.assertEquals(tsDimensions.size(), 2);
            Assert.assertEquals(tsDimensions.get(0).getTs(), 1122);
            Assert.assertEquals(tsDimensions.get(1).getTs(), ts.getTime());
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            Map<String, Object> keyMap = new HashMap<String, Object>();
            keyMap.put("card", "card1");
            keyMap.put("mcc", "mcc1");
            keyMap.put("amt", 1.5d);
            Object[] arrayRow = new Object[th.getSchema().size()];
            List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
            TableClientCommon.parseMapInput(keyMap, th, arrayRow, tsDimensions);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        try {
            Common.ColumnDesc col6 = Common.ColumnDesc.newBuilder().setName("ts1").setType("int64").build();
            Common.ColumnDesc col7 = Common.ColumnDesc.newBuilder().setName("ts2").setType("timestamp").build();
            NS.TableInfo table1 = NS.TableInfo.newBuilder()
                    .setName("test123").setTtl(14400)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                    .addColumnDescV1(col6).addColumnDescV1(col7)
                    .build();
            th = new TableHandler(table1);
            th.setPartitions( new PartitionHandler[] {new PartitionHandler(), new PartitionHandler(), new PartitionHandler()});
            Map<String, Object> keyMap = new HashMap<String, Object>();
            keyMap.put("card", "card1");
            keyMap.put("mcc", "mcc1");
            keyMap.put("amt", 1.5d);
            keyMap.put("col1", 1234);
            keyMap.put("ts1", 1122l);
            keyMap.put("ts2", Timestamp.valueOf("2019-05-30 19:50:43"));
            Object[] arrayRow = new Object[th.getSchema().size()];
            List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
            TableClientCommon.parseMapInput(keyMap, th, arrayRow, tsDimensions);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
}
