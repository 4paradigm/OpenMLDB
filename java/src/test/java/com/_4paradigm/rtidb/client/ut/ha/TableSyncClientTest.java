package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.ClientBuilder;
import com._4paradigm.rtidb.client.base.Config;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.BlobData;
import com._4paradigm.rtidb.client.impl.RelationalIterator;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.IndexDef;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.TableDesc;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.client.type.IndexType;
import com._4paradigm.rtidb.client.type.TableType;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com._4paradigm.rtidb.tablet.Tablet;
import com.google.protobuf.ByteString;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class TableSyncClientTest extends TestCaseBase {
    private static AtomicInteger id = new AtomicInteger(90000);
    private static String[] nodes = Config.NODES;

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    private String createKvTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();

        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0).build();

        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    private String createSchemaTable(String ttlType) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        PartitionMeta pm0_2 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(false).build();
        PartitionMeta pm0_3 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(true).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_3).addPartitionMeta(pm0_2).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().setTtlType(ttlType).addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(10)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    private String createSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }


    private String createTsSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);

        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("amt").setType("double").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("ts").setIsTsCol(true).setType("timestamp").build();
        Common.ColumnKey key = Common.ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setSegCnt(8).setName(name).setTtl(0).addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnKey(key)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    private String createRelationalTable(IndexType indexType) throws TabletException {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("id");
            col.setDataType(DataType.BigInt);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute");
            col.setDataType(DataType.Varchar);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("image");
            col.setDataType(DataType.Blob);
            col.setNotNull(false);
            list.add(col);
        }
        tableDesc.setColumnDescList(list);

        List<IndexDef> indexs = new ArrayList<>();
        IndexDef indexDef = new IndexDef();
        indexDef.setIndexName("idx1");
        indexDef.setIndexType(indexType);
        List<String> colNameList = new ArrayList<>();
        colNameList.add("id");
        indexDef.setColNameList(colNameList);
        indexs.add(indexDef);

        tableDesc.setIndexs(indexs);
        boolean ok = nsc.createTable(tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();

        return name;
    }

    private String createRelationalTablePkNotFirstIndex() throws TabletException {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute");
            col.setDataType(DataType.BigInt);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("id");
            col.setDataType(DataType.BigInt);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("image");
            col.setDataType(DataType.Blob);
            col.setNotNull(false);
            list.add(col);
        }
        tableDesc.setColumnDescList(list);

        List<IndexDef> indexs = new ArrayList<>();
        IndexDef indexDef = new IndexDef();
        indexDef.setIndexName("id");
        indexDef.setIndexType(IndexType.AutoGen);
        List<String> colNameList = new ArrayList<>();
        colNameList.add("id");
        indexDef.setColNameList(colNameList);
        indexs.add(indexDef);

        tableDesc.setIndexs(indexs);
        boolean ok = nsc.createTable(tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();

        return name;
    }

    class RelationTestArgs {
        TableDesc tableDesc;
        List<Object> conditionList;
        Object[] row;
        Object[] expected;
    }

    private RelationTestArgs createRelationalArgs(Object[] input, List<Object> conditionList, Object[] expect) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("id");
            col.setDataType(DataType.BigInt);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute");
            col.setDataType(DataType.Varchar);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("image");
            col.setDataType(DataType.Blob);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("memory");
            col.setDataType(DataType.Int);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("price");
            col.setDataType(DataType.Double);
            col.setNotNull(false);
            list.add(col);
        }
        tableDesc.setColumnDescList(list);

        List<IndexDef> indexs = new ArrayList<>();
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("id");
            indexDef.setIndexType(IndexType.PrimaryKey);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("id");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("attribute");
            indexDef.setIndexType(IndexType.Unique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("attribute");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("memory");
            indexDef.setIndexType(IndexType.NoUnique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("memory");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        tableDesc.setIndexs(indexs);

        RelationTestArgs args = new RelationTestArgs();
        args.tableDesc = tableDesc;
        args.row = input;
        args.expected = expect;
        args.conditionList = conditionList;

        return args;
    }

    private RelationTestArgs createRelationalWithCombineKeyArgs(Object[] input, List<Object> conditionList, Object[] expect) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("id");
            col.setDataType(DataType.BigInt);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("name");
            col.setDataType(DataType.String);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("sex");
            col.setDataType(DataType.Bool);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute");
            col.setDataType(DataType.Varchar);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("image");
            col.setDataType(DataType.Blob);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("memory");
            col.setDataType(DataType.Int);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("price");
            col.setDataType(DataType.Double);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute2");
            col.setDataType(DataType.Date);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("ts");
            col.setDataType(DataType.Timestamp);
            col.setNotNull(false);
            list.add(col);
        }
        tableDesc.setColumnDescList(list);

        List<IndexDef> indexs = new ArrayList<>();
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("idx1");
            indexDef.setIndexType(IndexType.PrimaryKey);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("name");
            colNameList.add("id");
            colNameList.add("sex");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("idx2");
            indexDef.setIndexType(IndexType.Unique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("attribute");
            colNameList.add("attribute2");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("idx3");
            indexDef.setIndexType(IndexType.NoUnique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("memory");
            colNameList.add("ts");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        tableDesc.setIndexs(indexs);

        RelationTestArgs args = new RelationTestArgs();
        args.tableDesc = tableDesc;
        args.row = input;
        args.expected = expect;
        args.conditionList = conditionList;

        return args;
    }

    private String createRelationalTableMultiIndex() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("id");
            col.setDataType(DataType.BigInt);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute");
            col.setDataType(DataType.Varchar);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("image");
            col.setDataType(DataType.Blob);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("memory");
            col.setDataType(DataType.Int);
            col.setNotNull(false);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("price");
            col.setDataType(DataType.Double);
            col.setNotNull(false);
            list.add(col);
        }
        tableDesc.setColumnDescList(list);

        List<IndexDef> indexs = new ArrayList<>();
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("id");
            indexDef.setIndexType(IndexType.PrimaryKey);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("id");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("attribute");
            indexDef.setIndexType(IndexType.Unique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("attribute");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        {
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("memory");
            indexDef.setIndexType(IndexType.NoUnique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("memory");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);
        }
        tableDesc.setIndexs(indexs);
        boolean ok = nsc.createTable(tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();

        return name;
    }

    private String createRelationalTable(DataType pkDataType) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("id");
            col.setDataType(pkDataType);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("attribute");
            col.setDataType(DataType.Varchar);
            col.setNotNull(true);
            list.add(col);
        }
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("image");
            col.setDataType(DataType.Varchar);
            col.setNotNull(true);
            list.add(col);
        }
        tableDesc.setColumnDescList(list);

        List<IndexDef> indexs = new ArrayList<>();
        IndexDef indexDef = new IndexDef();
        indexDef.setIndexName("id");
        indexDef.setIndexType(IndexType.PrimaryKey);
        List<String> colNameList = new ArrayList<>();
        colNameList.add("id");
        indexDef.setColNameList(colNameList);
        indexs.add(indexDef);

        tableDesc.setIndexs(indexs);
        boolean ok = nsc.createTable(tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();

        return name;
    }

    @Test
    public void testPut() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test2", 9527, "value1");
            Assert.assertTrue(ok);
            ByteString bs = tableSyncClient.get(name, "test1");
            String value = new String(bs.toByteArray());
            Assert.assertEquals(value, "value0");
            bs = tableSyncClient.get(name, "test2");
            value = new String(bs.toByteArray());
            Assert.assertEquals(value, "value1");
            Thread.sleep(1000 * 5);
            List<TableInfo> tables = nsc.showTable(name);
            Assert.assertTrue(tables.get(0).getTablePartition(0).getRecordCnt() == 1);
            Assert.assertEquals(tables.get(0).getTablePartition(0).getRecordByteSize(), 235);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }

    }

    @Test
    public void testSchemaPut() {

        String name = createSchemaTable();
        try {
            boolean ok = tableSyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 9528, new Object[]{"card1", "mcc1", 9.2d});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            row = tableSyncClient.getRow(name, "card1", 9528);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testScan() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 9529, 1000);
            Assert.assertTrue(it.getCount() == 3);
            Assert.assertTrue(it.valid());
            byte[] buffer = new byte[6];
            it.getValue().get(buffer);
            String value = new String(buffer);
            Assert.assertEquals(value, "value2");
            it.next();

            Assert.assertTrue(it.valid());
            it.getValue().get(buffer);
            value = new String(buffer);
            Assert.assertEquals(value, "value1");
            it.next();

            Assert.assertTrue(it.valid());
            it.getValue().get(buffer);
            value = new String(buffer);
            Assert.assertEquals(value, "value0");
            it.next();

            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testDelete() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 9529, 1000);
            Assert.assertTrue(it.getCount() == 3);
            Assert.assertTrue(tableSyncClient.delete(name, "test1"));
            it = tableSyncClient.scan(name, "test1", 9529, 1000);
            Assert.assertTrue(it.getCount() == 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testDeleteSchema() {
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 9529, 1000);
            Assert.assertTrue(it.getCount() == 2);
            Assert.assertTrue(tableSyncClient.delete(name, "card0", "card"));
            it = tableSyncClient.scan(name, "card0", "card", 9529, 1000);
            Assert.assertTrue(it.getCount() == 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testCount() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test2", 9529, "value3");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test3", 9530, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test3", 9530, "value2");
            Assert.assertTrue(ok);
            Assert.assertEquals(3, tableSyncClient.count(name, "test1"));
            Assert.assertEquals(3, tableSyncClient.count(name, "test1", true));
            Assert.assertEquals(1, tableSyncClient.count(name, "test2"));
            Assert.assertEquals(1, tableSyncClient.count(name, "test2", true));
            Assert.assertEquals(2, tableSyncClient.count(name, "test3"));
            RTIDBClientConfig configA = client.getConfig();
            RTIDBClientConfig configB = new RTIDBClientConfig();
            configB.setZkEndpoints(configA.getZkEndpoints());
            configB.setZkRootPath(configA.getZkRootPath());
            configB.setNsEndpoint(configA.getNsEndpoint());
            configB.setReadTimeout(configA.getReadTimeout());
            configB.setWriteTimeout(configA.getWriteTimeout());
            configB.setRemoveDuplicateByTime(true);
            RTIDBClusterClient testNSc = new RTIDBClusterClient(configB);
            testNSc.init();
            TableSyncClient tableSyncClientB = new TableSyncClientImpl(testNSc);
            Assert.assertEquals(1, tableSyncClientB.count(name, "test3", true));
            testNSc.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTsCountSchema() {
        String name = createTsSchemaTable();
        try {
            long now = System.currentTimeMillis();
            boolean ok = tableSyncClient.put(name, new Object[]{"card1", 1.1d, new DateTime(now)});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, new Object[]{"card1", 2.1d, new DateTime(now - 1000)});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, new Object[]{"card1", 3.1d, new DateTime(now - 2000)});
            Assert.assertTrue(ok);
            Assert.assertEquals(3, tableSyncClient.count(name, "card1", "card", "ts", now, 0l));
            Assert.assertEquals(1, tableSyncClient.count(name, "card1", "card", "ts", now, now - 1000));
            Assert.assertEquals(2, tableSyncClient.count(name, "card1", "card", "ts", now, now - 2000));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testCountSchema() {
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc2");
            rowMap.put("amt", 9.25d);
            ok = tableSyncClient.put(name, 9529, rowMap);
            Assert.assertTrue(ok);
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc2");
            rowMap.put("amt", 9.3d);
            ok = tableSyncClient.put(name, 9529, rowMap);
            Assert.assertTrue(ok);
            Assert.assertEquals(2, tableSyncClient.count(name, "card0", "card"));
            Assert.assertEquals(2, tableSyncClient.count(name, "card0", "card", true));
            Assert.assertEquals(1, tableSyncClient.count(name, "mcc1", "mcc"));
            Assert.assertEquals(1, tableSyncClient.count(name, "mcc1", "mcc", true));
            Assert.assertEquals(2, tableSyncClient.count(name, "mcc2", "mcc"));
            Assert.assertEquals(2, tableSyncClient.count(name, "card1", "card"));
            RTIDBClientConfig configA = client.getConfig();
            RTIDBClientConfig configB = new RTIDBClientConfig();
            configB.setZkEndpoints(configA.getZkEndpoints());
            configB.setZkRootPath(configA.getZkRootPath());
            configB.setNsEndpoint(configA.getNsEndpoint());
            configB.setReadTimeout(configA.getReadTimeout());
            configB.setWriteTimeout(configA.getWriteTimeout());
            configB.setRemoveDuplicateByTime(true);
            RTIDBClusterClient testNSc = new RTIDBClusterClient(configB);
            testNSc.init();
            TableSyncClient tableSyncClientB = new TableSyncClientImpl(testNSc);
            Assert.assertEquals(1, tableSyncClientB.count(name, "mcc2", "mcc", true));
            Assert.assertEquals(1, tableSyncClientB.count(name, "card1", "card", true));
            testNSc.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testPutForIgnoreTime() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("timestamp").setIsTsCol(true).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts_1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts_1").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setPartitionNum(1).setReplicaNum(1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 5);
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.7);
            data.put("ts", Timestamp.valueOf("2018-11-22 01:10:22"));
            data.put("ts_1", 444l);
            tableSyncClient.put(name, 111111111l, data);

            Object[] row = tableSyncClient.getRow(name, "card0", "card", Timestamp.valueOf("2018-11-22 01:10:22").getTime(), "ts", null);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7);
            Assert.assertEquals(((DateTime) row[3]).getMillis(), Timestamp.valueOf("2018-11-22 01:10:22").getTime());
            Assert.assertEquals(row[4], 444l);

            Assert.assertTrue(tableSyncClient.put(name, 111111111l, new Object[]{"card1", "mcc1", 1.7, Timestamp.valueOf("2018-11-22 01:10:22"), 444l}));
            row = tableSyncClient.getRow(name, "card1", "card", 444l, "ts_1", null);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7);
            Assert.assertEquals(((DateTime) row[3]).getMillis(), Timestamp.valueOf("2018-11-22 01:10:22").getTime());
            Assert.assertEquals(row[4], 444l);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void TestWriteBinary() {
        String name = "";
        try {
            name = createRelationalTable(IndexType.PrimaryKey);
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", "a1");

            byte[] bytedata = "Any String you want".getBytes();
            ByteBuffer buf = ByteBuffer.allocate(bytedata.length);
            for (int i = 0; i < bytedata.length; i++) {
                buf.put(bytedata[i]);
            }
            data.put("image", buf);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
            Map<String, Object> queryMap;
            //query
            {
                Map<String, Object> index = new HashMap<>();
                index.put("id", 11l);
                Set<String> colSet = new HashSet<>();
                colSet.add("id");
                colSet.add("image");
                ReadOption ro = new ReadOption(index, null, colSet, 1);
                RelationalIterator it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertTrue(buf.equals(((BlobData) queryMap.get("image")).getData()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    ByteBuffer StringToBB(String ss) {
        ByteBuffer buf = ByteBuffer.allocate(ss.getBytes().length);
        for (int k = 0; k < ss.getBytes().length; k++) {
            buf.put(ss.getBytes()[k]);
        }
        buf.rewind();
        return buf;
    }

    @Test
    public void TestCreateRelationTableErrorMultiPk() {
        String name = "";
        try {
            name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            TableDesc tableDesc = new TableDesc();
            tableDesc.setName(name);
            tableDesc.setTableType(TableType.kRelational);
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
            {
                com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
                col.setName("id");
                col.setDataType(DataType.BigInt);
                col.setNotNull(true);
                list.add(col);
            }
            {
                com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
                col.setName("attribute");
                col.setDataType(DataType.Varchar);
                col.setNotNull(true);
                list.add(col);
            }
            {
                com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
                col.setName("image");
                col.setDataType(DataType.Blob);
                col.setNotNull(false);
                list.add(col);
            }
            tableDesc.setColumnDescList(list);

            List<IndexDef> indexs = new ArrayList<>();
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("idx1");
            indexDef.setIndexType(IndexType.PrimaryKey);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("id");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);

            indexDef = new IndexDef();
            indexDef.setIndexName("idx2");
            indexDef.setIndexType(IndexType.AutoGen);
            colNameList = new ArrayList<>();
            colNameList.add("attribute");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);

            tableDesc.setIndexs(indexs);
            boolean ok = nsc.createTable(tableDesc);
            Assert.assertFalse(ok);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void TestCreateRelationTableErrorNoPk() {
        String name = "";
        try {
            name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            TableDesc tableDesc = new TableDesc();
            tableDesc.setName(name);
            tableDesc.setTableType(TableType.kRelational);
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
            {
                com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
                col.setName("id");
                col.setDataType(DataType.BigInt);
                col.setNotNull(true);
                list.add(col);
            }
            {
                com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
                col.setName("attribute");
                col.setDataType(DataType.Varchar);
                col.setNotNull(true);
                list.add(col);
            }
            {
                com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
                col.setName("image");
                col.setDataType(DataType.Blob);
                col.setNotNull(false);
                list.add(col);
            }
            tableDesc.setColumnDescList(list);

            List<IndexDef> indexs = new ArrayList<>();
            IndexDef indexDef = new IndexDef();
            indexDef.setIndexName("idx1");
            indexDef.setIndexType(IndexType.NoUnique);
            List<String> colNameList = new ArrayList<>();
            colNameList.add("id");
            indexDef.setColNameList(colNameList);
            indexs.add(indexDef);

            tableDesc.setIndexs(indexs);
            boolean ok = nsc.createTable(tableDesc);
            Assert.assertFalse(ok);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTable() {
        String name = "";
        try {
            name = createRelationalTable(IndexType.PrimaryKey);
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", "a1");
            String imageData1 = "i1";
            ByteBuffer buf1 = StringToBB(imageData1);
            data.put("image", buf1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            data.clear();
            data.put("id", 12l);
            data.put("attribute", "a2");
            String imageData2 = "i1";
            ByteBuffer buf2 = StringToBB(imageData2);
            data.put("image", buf2);
            tableSyncClient.put(name, data, wo);

            data.clear();
            data.put("id", 12l);
            data.put("attribute", "a2");
            imageData2 = "i1";
            buf2 = StringToBB(imageData2);
            data.put("image", buf2);
            try {
                tableSyncClient.put(name, data, wo);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            ReadOption ro;
            RelationalIterator it;
            Map<String, Object> queryMap;
            //query
            {
                Map<String, Object> index = new HashMap<>();
                index.put("id", 11l);
                Set<String> colSet = new HashSet<>();
                colSet.add("id");
                colSet.add("image");
                ro = new ReadOption(index, null, colSet, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));
            }
            {
                Map<String, Object> index2 = new HashMap<>();
                index2.put("id", 12l);
                ro = new ReadOption(index2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
            }
            // query error
            {
                Map<String, Object> index2 = new HashMap<>();
                index2.put("attribute", "a1");
                ro = new ReadOption(index2, null, null, 1);
                try {
                    it = tableSyncClient.query(name, ro);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            //batch query
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 12l);
                    Set<String> colSet = new HashSet<>();
                    colSet.add("id");
                    colSet.add("image");
                    ro = new ReadOption(index, null, colSet, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("id", 11l);
                    Set<String> colSet = new HashSet<>();
                    colSet.add("id");
                    ro = new ReadOption(index2, null, colSet, 1);
                    ros.add(ro);
                }
                try {
                    it = tableSyncClient.batchQuery(name, ros);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
                ros.clear();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 12l);
                    Set<String> colSet = new HashSet<>();
                    colSet.add("id");
                    colSet.add("image");
                    ro = new ReadOption(index, null, colSet, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("id", 11l);
                    Set<String> colSet = new HashSet<>();
                    colSet.add("id");
                    colSet.add("image");
                    ro = new ReadOption(index2, null, null, 1);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertEquals(it.getCount(), 2);

                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));

                it.next();
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));
                it.next();
                Assert.assertFalse(it.valid());

                ros.clear();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 12l);
                    Set<String> colSet = new HashSet<>();
                    colSet.add("id");
                    colSet.add("image");
                    ro = new ReadOption(index, null, colSet, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("id", 11l);
                    ro = new ReadOption(index2, null, null, 1);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertEquals(it.getCount(), 2);

                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));

                it.next();
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));

                it.next();
                Assert.assertFalse(it.valid());
            }
            //batchQuery error
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("price", 11.1);
                    ro = new ReadOption(index, null, null, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("id", 11l);
                    ro = new ReadOption(index2, null, null, 1);
                    ros.add(ro);
                }
                try {
                    it = tableSyncClient.batchQuery(name, ros);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            //batch query partial
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 18l);
                    ro = new ReadOption(index, null, null, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("id", 11l);
                    ro = new ReadOption(index2, null, null, 1);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertEquals(it.getCount(), 1);
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));

                it.next();
                Assert.assertFalse(it.valid());
            }
            //batch query empty
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 18l);
                    ro = new ReadOption(index, null, null, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("id", 17l);
                    ro = new ReadOption(index2, null, null, 1);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertFalse(it.valid());
            }
            boolean ok = false;
            UpdateResult updateResult;
            String imageData3 = "i3";
            ByteBuffer buf3 = StringToBB(imageData3);
            //update
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", 11l);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("attribute", "a3");

                valueColumns.put("image", buf3);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                Map<String, Object> index = new HashMap<>();
                index.put("id", 11l);
                ro = new ReadOption(index, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf3.equals(((BlobData) queryMap.get("image")).getData()));
            }
            {
                //update empty
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", 18l);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("attribute", "a3");

                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            {
                //update error
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("price", 11.1);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("attribute", "a3");
                try {
                    updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("id", 12l);
                Map<String, Object> valueColumns2 = new HashMap<>();
                valueColumns2.put("attribute", "a3");
                updateResult = tableSyncClient.update(name, conditionColumns2, valueColumns2, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                Map<String, Object> index2 = new HashMap<>();
                index2.put("id", 12l);
                ro = new ReadOption(index2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));

            }
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("id", 12l);
                Map<String, Object> valueColumns2 = new HashMap<>();
                valueColumns2.put("attribute", null);
                try {
                    tableSyncClient.update(name, conditionColumns2, valueColumns2, wo);
                    Assert.assertTrue(false);
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("id", 12l);
                Map<String, Object> valueColumns2 = new HashMap<>();
                valueColumns2.put("image", null);
                updateResult = tableSyncClient.update(name, conditionColumns2, valueColumns2, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                Map<String, Object> index3 = new HashMap<>();
                index3.put("id", 12l);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertEquals(queryMap.get("image"), null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableWithMultiIndex() {
        String name = "";
        try {
            name = createRelationalTableMultiIndex();
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 5);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", "a1");
            ByteBuffer buf1 = StringToBB("i1");
            data.put("image", buf1);
            data.put("memory", 11);
            data.put("price", 11.1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            data.clear();
            data.put("id", 12l);
            data.put("attribute", "a2");
            ByteBuffer buf2 = StringToBB("i2");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            data.clear();
            data.put("id", 12l);
            data.put("attribute", "a2");
            buf2 = StringToBB("i2");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            try {
                tableSyncClient.put(name, data, wo);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(true);
            }

            //query
            ReadOption ro;
            RelationalIterator it;
            Map<String, Object> queryMap;
            {
                Map<String, Object> index = new HashMap<>();
                index.put("id", 10l);
                Set<String> colSet = new HashSet<>();
                colSet.add("id");
                colSet.add("image");
                colSet.add("price");
                ro = new ReadOption(index, null, colSet, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
            {
                Map<String, Object> index = new HashMap<>();
                index.put("id", 11l);
                Set<String> colSet = new HashSet<>();
                colSet.add("id");
                colSet.add("image");
                colSet.add("price");
                ro = new ReadOption(index, null, colSet, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("price"), 11.1);
            }
            // query error
            {
                Map<String, Object> index2 = new HashMap<>();
                index2.put("price", 11.1);
                ro = new ReadOption(index2, null, null, 1);
                try {
                    it = tableSyncClient.query(name, ro);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            {
                Map<String, Object> index2 = new HashMap<>();
                index2.put("id", 12l);
                ro = new ReadOption(index2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);
            }
            {
                //query no unique
                Map<String, Object> index3 = new HashMap<>();
                index3.put("attribute", "a2");
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);
            }
            {
                Map<String, Object> index3 = new HashMap<>();
                index3.put("attribute", "aa");
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
            {
                Map<String, Object> index3 = new HashMap<>();
                index3.put("memory", 10);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
            {
                Map<String, Object> index3 = new HashMap<>();
                index3.put("memory", 12);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 1);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertFalse(it.valid());
            }

            //put second repeated
            data.clear();
            data.put("id", 13l);
            data.put("attribute", "a2");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            try {
                tableSyncClient.put(name, data, wo);
                Assert.assertTrue(false);
            } catch (TabletException e) {
                Assert.assertTrue(true);
            }

            data.clear();
            data.put("id", 13l);
            data.put("attribute", "a3");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            {
                //query no unique
                Map<String, Object> index3 = new HashMap<>();
                index3.put("memory", 12);
                ro = new ReadOption(index3, null, null, 2);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 2);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertFalse(it.valid());
            }

            //batch query
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 12l);
                    ro = new ReadOption(index, null, null, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("memory", 12);
                    ro = new ReadOption(index2, null, null, 2);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertEquals(it.getCount(), 3);

                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                queryMap = it.getDecodedValue();
                Assert.assertTrue(it.valid());
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertFalse(it.valid());
            }
            //batchQuery partial
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("id", 18l);
                    ro = new ReadOption(index, null, null, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("memory", 12);
                    ro = new ReadOption(index2, null, null, 2);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertEquals(it.getCount(), 2);

                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertFalse(it.valid());
            }
            //batch query error
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("price", 12.2);
                    ro = new ReadOption(index, null, null, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("memory", 12);
                    ro = new ReadOption(index2, null, null, 2);
                    ros.add(ro);
                }
                try {
                    it = tableSyncClient.batchQuery(name, ros);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }

            boolean ok = false;
            UpdateResult updateResult;
            //update by pk
            ByteBuffer buf3 = StringToBB("i3");
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", 12l);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 13.3);
                valueColumns.put("image", buf3);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query pk
                Map<String, Object> index3 = new HashMap<>();
                index3.put("id", 12l);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertEquals(((BlobData) queryMap.get("image")).getData(), buf3);
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 13.3);
            }
            {
                // update empty
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", 18l);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 13.3);
                valueColumns.put("image", buf3);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            {
                // update error
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("price", 13.3);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 13.3);
                valueColumns.put("image", buf3);
                try {
                    updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            //update by unique
            ByteBuffer buf4 = StringToBB("i4");
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("attribute", "a2");
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 14.4);
                valueColumns.put("image", buf4);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query unique
                Map<String, Object> index3 = new HashMap<>();
                index3.put("attribute", "a2");
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf4.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 14.4);
            }
            //update by no unique empty
            ByteBuffer buf5 = StringToBB("i5");
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("memory", 18);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 15.5);
                valueColumns.put("image", buf5);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            //update by no unique
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("memory", 12);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 15.5);
                valueColumns.put("image", buf5);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 2);

                //query no unique
                Map<String, Object> index3 = new HashMap<>();
                index3.put("memory", 12);
                ro = new ReadOption(index3, null, null, 2);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 2);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(buf5.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 15.5);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf5.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 15.5);
            }
            //update pk by no unique
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("memory", 12);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("id", 16l);
                valueColumns.put("price", 16.6);
                try {
                    updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            //update unique by no unique
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("memory", 12);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("id", 16l);
                valueColumns.put("price", 16.6);
                try {
                    updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableNullIndex() {
        String name = "";
        try {
            name = createRelationalTableMultiIndex();
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 5);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", "a1");
            ByteBuffer buf1 = StringToBB("i1");
            data.put("image", buf1);
            data.put("memory", null);
            data.put("price", 11.1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            data.clear();
            data.put("id", 12l);
            data.put("attribute", null);
            ByteBuffer buf2 = StringToBB("i2");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            //query
            ReadOption ro;
            RelationalIterator it;
            Map<String, Object> queryMap;
            {
                //query no unique
                Map<String, Object> index3 = new HashMap<>();
                index3.put("attribute", null);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), null);
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);
            }
            {
                Map<String, Object> index3 = new HashMap<>();
                index3.put("memory", null);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 1);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), null);
                Assert.assertEquals(queryMap.get("price"), 11.1);

                it.next();
                Assert.assertFalse(it.valid());
            }

            data.clear();
            data.put("id", 13l);
            data.put("attribute", "a3");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);
            {
                //query no unique
                Map<String, Object> index3 = new HashMap<>();
                index3.put("memory", 12);
                ro = new ReadOption(index3, null, null, 2);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 2);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), null);
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);

                it.next();
                Assert.assertFalse(it.valid());
            }
            // traverse
            {
                ro = new ReadOption(null, null, null, 1);
                it = tableSyncClient.traverse(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 3);

                queryMap = it.getDecodedValue();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 11l);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 13l);
            }
            // update
            UpdateResult updateResult;
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("attribute", null);
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 14.4);
                updateResult = tableSyncClient.update(name, conditionColumns, valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query
                Map<String, Object> index3 = new HashMap<>();
                index3.put("attribute", null);
                ro = new ReadOption(index3, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 5);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("attribute"), null);
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 14.4);
            }
            //delete
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("attribute", null);
                updateResult = tableSyncClient.delete(name, conditionColumns2);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption(conditionColumns2, null, null, 0);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @DataProvider(name = "relational_delete_case")
    public Object[][] genDeleteCase() {
        Object[] arr = new Object[3];
        Map data = new HashMap<String, Object>();
        data.put("id", 11l);
        data.put("attribute", "a1");
        data.put("image", StringToBB("i1"));
        data.put("memory", 11);
        data.put("price", 11.1);
        arr[0] = new HashMap<>(data);

        List<Object> list = new ArrayList<>();
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("id", 12l);
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("price", 11.1);
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("id", 11l);
            list.add(conditionColumns);
        }

        return new Object[][]{
                new Object[]{createRelationalArgs(arr, list, new Object[]{5, 1, 0})}
        };
    }

    @Test(dataProvider = "relational_delete_case")
    public void testRelationalPkDelete(RelationTestArgs args) {
        nsc.dropTable(args.tableDesc.getName());
        boolean ok = nsc.createTable(args.tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        String name = args.tableDesc.getName();

        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), args.expected[0]);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            Assert.assertTrue(tableSyncClient.put(args.tableDesc.getName(), (Map) (args.row[0]), wo).isSuccess());

            //traverse
            ReadOption ro = new ReadOption(null, null, null, 1);
            RelationalIterator it = tableSyncClient.traverse(name, ro);
            Assert.assertEquals(it.getCount(), args.expected[1]);

            //delete pk
            UpdateResult updateResult;
            {
                // delete empty
                updateResult = tableSyncClient.delete(name, (Map) args.conditionList.get(0));
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            {
                // delete error
                try {
                    tableSyncClient.delete(name, (Map) args.conditionList.get(1));
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            {
                updateResult = tableSyncClient.delete(name, (Map) args.conditionList.get(2));
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption((Map) args.conditionList.get(0), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
            //traverse
            it = tableSyncClient.traverse(name, ro);
            Assert.assertEquals(it.getCount(), args.expected[2]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @DataProvider(name = "relational_combine_key_case")
    public Object[][] genCombineKeyCase() {
        Object[] arr = new Object[4];
        ByteBuffer buf1 = StringToBB("i1");
        ByteBuffer buf2 = StringToBB("i2");
        ByteBuffer buf3 = StringToBB("i3");
        ByteBuffer buf4 = StringToBB("i4");

        {

            Map data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("name", "n1");
            data.put("sex", true);
            data.put("attribute", "a1");
            data.put("image", buf1);
            data.put("memory", 11);
            data.put("price", 11.1);
            data.put("attribute2", new Date(2020, 5, 1));
            data.put("ts", new DateTime(1588756531));
            arr[0] = data;
        }
        {
            Map data = new HashMap<String, Object>();
            data.put("id", 12l);
            data.put("name", "n2");
            data.put("sex", false);
            data.put("attribute", "a2");
            data.put("image", buf2);
            data.put("memory", 12);
            data.put("price", 12.2);
            data.put("attribute2", new Date(2020, 5, 2));
            data.put("ts", new DateTime(1588756532));
            arr[1] = data;
        }
        {
            Map data = new HashMap<String, Object>();
            data.put("id", 13l);
            data.put("name", "n3");
            data.put("sex", true);
            data.put("attribute", "a3");
            data.put("image", buf3);
            data.put("memory", 12);
            data.put("price", 13.3);
            data.put("attribute2", new Date(2020, 5, 3));
            data.put("ts", new DateTime(1588756532));
            arr[2] = data;
        }
        {
            Map data = new HashMap<String, Object>();
            data.put("id", 14l);
            data.put("name", "n4");
            data.put("sex", false);
            data.put("attribute", "a4");
            data.put("image", buf4);
            data.put("memory", 14);
            data.put("price", 14.4);
            data.put("attribute2", new Date(2020, 5, 4));
            data.put("ts", new DateTime(1588756534));
            arr[3] = data;
        }

        List<Object> list = new ArrayList<>();
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("id", 11l);
            conditionColumns.put("name", "n1");
            conditionColumns.put("sex", true);
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("attribute", "a1");
            conditionColumns.put("attribute2", new Date(2020, 5, 1));
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("memory", 12);
            conditionColumns.put("ts", new DateTime(1588756532));
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("id", 15l);
            conditionColumns.put("name", "n1");
            conditionColumns.put("sex", true);
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("attribute", "a5");
            conditionColumns.put("attribute2", new Date(2020, 5, 1));
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("memory", 12);
            conditionColumns.put("ts", new DateTime(1588756535));
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("id", 18l);
            conditionColumns.put("name", "n1");
            conditionColumns.put("sex", true);
            list.add(conditionColumns);
        }
        {
            Map<String, Object> conditionColumns = new HashMap<>();
            conditionColumns.put("id", 18l);
            conditionColumns.put("name", "n1");
            list.add(conditionColumns);
        }

        return new Object[][]{
                new Object[]{createRelationalWithCombineKeyArgs(arr, list, new Object[]{1, 1, 2, 3, 1, 1, 2})}
        };
    }

    @Test(dataProvider = "relational_combine_key_case")
    public void testRelationalTableWithCombineKey(RelationTestArgs args) {
        nsc.dropTable(args.tableDesc.getName());
        boolean ok = nsc.createTable(args.tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        String name = args.tableDesc.getName();
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 9);
            //put
            WriteOption wo = new WriteOption();
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[0]), wo).isSuccess());
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[1]), wo).isSuccess());
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[2]), wo).isSuccess());
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[3]), wo).isSuccess());
            //query
            ReadOption ro;
            RelationalIterator it;
            Map<String, Object> queryMap;
            {
                //query pk
                ro = new ReadOption((Map) args.conditionList.get(0), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[0]);
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(StringToBB("i1").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 11.1);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            {
                //query unique
                ro = new ReadOption((Map) args.conditionList.get(1), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[1]);
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(StringToBB("i1").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 11.1);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            {
                //query no unique
                ro = new ReadOption((Map) args.conditionList.get(2), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[2]);
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("name"), "n2");
                Assert.assertEquals(queryMap.get("sex"), false);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(StringToBB("i2").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 2));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756532));

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("name"), "n3");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(StringToBB("i3").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 13.3);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 3));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756532));

                it.next();
                Assert.assertFalse(it.valid());
            }
            {
                //batch query
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    ro = new ReadOption((Map) args.conditionList.get(0), null, null, 1);
                    ros.add(ro);
                }
                {
                    ro = new ReadOption((Map) args.conditionList.get(2), null, null, 1);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[3]);
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(StringToBB("i1").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 11.1);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("name"), "n2");
                Assert.assertEquals(queryMap.get("sex"), false);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(StringToBB("i2").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 12.2);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 2));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756532));

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("name"), "n3");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(StringToBB("i3").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 13.3);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 3));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756532));

                it.next();
                Assert.assertFalse(it.valid());
            }

            //update by pk
            UpdateResult updateResult;
            {
                // update empty
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 15.5);
                valueColumns.put("image", StringToBB("i5"));
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(6), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            {
                // update error
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 15.5);
                valueColumns.put("image", StringToBB("i5"));
                try {
                    updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(7), valueColumns, wo);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            {
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 15.5);
                valueColumns.put("image", StringToBB("i5"));
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(0), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query pk
                ro = new ReadOption((Map) args.conditionList.get(0), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[4]);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(StringToBB("i5").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 15.5);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            //update by unique
            {
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 16.6);
                valueColumns.put("image", StringToBB("i6"));
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(1), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query unique
                ro = new ReadOption((Map) args.conditionList.get(1), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[5]);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 11l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(StringToBB("i6").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 16.6);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            //update by no unique
            {
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 17.7);
                valueColumns.put("image", StringToBB("i7"));
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(2), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 2);

                //query no unique
                ro = new ReadOption((Map) args.conditionList.get(2), null, null, 2);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[6]);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("name"), "n2");
                Assert.assertEquals(queryMap.get("sex"), false);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(StringToBB("i7").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 17.7);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 2));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756532));

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("name"), "n3");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(StringToBB("i7").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 17.7);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 3));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756532));

                it.next();
                Assert.assertFalse(it.valid());
            }
            {
                //delete by pk
                ro = new ReadOption(null, null, null, 1);
                it = tableSyncClient.traverse(name, ro);
                Assert.assertEquals(it.getCount(), 4);

                updateResult = tableSyncClient.delete(name, (Map) args.conditionList.get(0));
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption((Map) args.conditionList.get(0), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());

                ro = new ReadOption(null, null, null, 1);
                it = tableSyncClient.traverse(name, ro);
                Assert.assertEquals(it.getCount(), 3);
            }
            {
                //delete empty
                updateResult = tableSyncClient.delete(name, (Map) args.conditionList.get(6));
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            {
                //delete error
                try {
                    tableSyncClient.delete(name, (Map) args.conditionList.get(7));
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            //put second
            ((Map) (args.row[0])).put("image", StringToBB("i1"));
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[0]), wo).isSuccess());
            {
                //delete by unique
                ro = new ReadOption(null, null, null, 1);
                it = tableSyncClient.traverse(name, ro);
                Assert.assertEquals(it.getCount(), 4);

                updateResult = tableSyncClient.delete(name, (Map) args.conditionList.get(1));
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption((Map) args.conditionList.get(1), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());

                ro = new ReadOption(null, null, null, 1);
                it = tableSyncClient.traverse(name, ro);
                Assert.assertEquals(it.getCount(), 3);
            }
            {
                //delete by no unique
                updateResult = tableSyncClient.delete(name, (Map) args.conditionList.get(2));
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 2);
                ro = new ReadOption((Map) args.conditionList.get(2), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());

                ro = new ReadOption(null, null, null, 1);
                it = tableSyncClient.traverse(name, ro);
                Assert.assertEquals(it.getCount(), 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test(dataProvider = "relational_combine_key_case")
    public void testRelationalTableUpdateIndex(RelationTestArgs args) {
        nsc.dropTable(args.tableDesc.getName());
        boolean ok = nsc.createTable(args.tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        String name = args.tableDesc.getName();
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 9);
            //put
            WriteOption wo = new WriteOption();
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[0]), wo).isSuccess());
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[1]), wo).isSuccess());
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[2]), wo).isSuccess());
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[3]), wo).isSuccess());
            //query
            ReadOption ro;
            RelationalIterator it;
            Map<String, Object> queryMap;
            //update by pk
            UpdateResult updateResult;
            {
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("id", 15l);
                valueColumns.put("image", StringToBB("i5"));
                valueColumns.put("price", 15.5);
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(0), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query pk
                ro = new ReadOption((Map) args.conditionList.get(0), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
                ro = new ReadOption((Map) args.conditionList.get(3), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 1);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 15l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a1");
                Assert.assertTrue(StringToBB("i5").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 15.5);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            //update by unique
            {
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("attribute", "a5");
                valueColumns.put("price", 16.6);
                valueColumns.put("image", StringToBB("i6"));
                valueColumns.put("sex", false);
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(1), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                //query unique
                ro = new ReadOption((Map) args.conditionList.get(4), null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 1);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 15l);
                Assert.assertEquals(queryMap.get("name"), "n1");
                Assert.assertEquals(queryMap.get("sex"), false);
                Assert.assertEquals(queryMap.get("attribute"), "a5");
                Assert.assertTrue(StringToBB("i6").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 16.6);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            //update by no unique
            {
                Map<String, Object> valueColumns = new HashMap<>();
                valueColumns.put("price", 17.7);
                valueColumns.put("image", StringToBB("i7"));
                valueColumns.put("ts", new DateTime(1588756535));
                updateResult = tableSyncClient.update(name, (Map) args.conditionList.get(2), valueColumns, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 2);

                //query no unique
                ro = new ReadOption((Map) args.conditionList.get(5), null, null, 2);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), args.expected[6]);

                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 12l);
                Assert.assertEquals(queryMap.get("name"), "n2");
                Assert.assertEquals(queryMap.get("sex"), false);
                Assert.assertEquals(queryMap.get("attribute"), "a2");
                Assert.assertTrue(StringToBB("i7").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 17.7);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 2));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756535));

                it.next();
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 9);
                Assert.assertEquals(queryMap.get("id"), 13l);
                Assert.assertEquals(queryMap.get("name"), "n3");
                Assert.assertEquals(queryMap.get("sex"), true);
                Assert.assertEquals(queryMap.get("attribute"), "a3");
                Assert.assertTrue(StringToBB("i7").equals(((BlobData) queryMap.get("image")).getData()));
                Assert.assertEquals(queryMap.get("memory"), 12);
                Assert.assertEquals(queryMap.get("price"), 17.7);
                Assert.assertEquals(queryMap.get("attribute2"), new Date(2020, 5, 3));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756535));

                it.next();
                Assert.assertFalse(it.valid());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalDelete() {
        String name = "";
        try {
            name = createRelationalTableMultiIndex();
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 5);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", "a1");
            data.put("image", "i1");
            data.put("image", StringToBB("i1"));
            data.put("memory", 11);
            data.put("price", 11.1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            //traverse
            ReadOption ro = new ReadOption(null, null, null, 1);
            RelationalIterator it = tableSyncClient.traverse(name, ro);
            Assert.assertEquals(it.getCount(), 1);

            boolean ok = false;
            UpdateResult updateResult;
            //delete pk
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("id", 11l);
                updateResult = tableSyncClient.delete(name, conditionColumns2);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption(conditionColumns2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
            //traverse
            it = tableSyncClient.traverse(name, ro);
            Assert.assertEquals(it.getCount(), 0);

            data.clear();
            data.put("id", 11l);
            data.put("attribute", "a1");
            data.put("memory", 11);
            data.put("image", StringToBB("i1"));
            data.put("price", 11.1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            data.clear();
            data.put("id", 12l);
            data.put("attribute", "a2");
            data.put("image", StringToBB("i2"));
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            data.clear();
            data.put("id", 13l);
            data.put("attribute", "a3");
            data.put("image", StringToBB("i2"));
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            //traverse
            ro = new ReadOption(null, null, null, 1);
            it = tableSyncClient.traverse(name, ro);
            Assert.assertEquals(it.getCount(), 3);

            //delete unique
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("attribute", "a3");
                updateResult = tableSyncClient.delete(name, conditionColumns2);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption(conditionColumns2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
            it = tableSyncClient.traverse(name, (ReadOption) null);
            Assert.assertEquals(it.getCount(), 2);
            Assert.assertTrue(it.valid());
            Map queryMap = it.getDecodedValue();
            Assert.assertEquals(queryMap.get("attribute"), "a1");

            it.next();
            Assert.assertTrue(it.valid());
            queryMap = it.getDecodedValue();
            Assert.assertEquals(queryMap.get("attribute"), "a2");

            data.clear();
            data.put("id", 13l);
            data.put("attribute", "a3");
            data.put("image", StringToBB("i2"));
            data.put("memory", 12);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            data.clear();
            data.put("id", 14l);
            data.put("attribute", "a4");
            data.put("image", StringToBB("i2"));
            data.put("memory", 13);
            data.put("price", 12.2);
            tableSyncClient.put(name, data, wo);

            //traverse
            it = tableSyncClient.traverse(name, (ReadOption) null);
            Assert.assertEquals(it.getCount(), 4);

            //delete no unique
            {
                // delete empty
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", 18l);
                updateResult = tableSyncClient.delete(name, conditionColumns);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 0);
            }
            {
                // delete error
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("price", 11.1);
                try {
                    tableSyncClient.delete(name, conditionColumns);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }
            }
            {
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("memory", 12);
                updateResult = tableSyncClient.delete(name, conditionColumns);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 2);
                ro = new ReadOption(conditionColumns, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());

                Thread.currentThread().sleep(3000);
                TableInfo tableInfo = nsc.showTable(name).get(0);
                long countA[] = new long[1];
                long countB[] = new long[1];
                getRecordCount(tableInfo, countA, countB);
                Assert.assertEquals(countA[0], 2);
                Assert.assertEquals(countA[0], countB[0]);

                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("memory", 13);
                ro = new ReadOption(conditionColumns2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 1);
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.get("memory"), 13);
            }
            it = tableSyncClient.traverse(name, new ReadOption(null, null, null, 0));
            Assert.assertEquals(it.getCount(), 2);
            Assert.assertTrue(it.valid());
            queryMap = it.getDecodedValue();
            Assert.assertEquals(queryMap.get("memory"), 11);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test(dataProvider = "relational_combine_key_case")
    public void testRelationalTableTraverseWithCombineKey(RelationTestArgs args) {
        nsc.dropTable(args.tableDesc.getName());
        boolean ok = nsc.createTable(args.tableDesc);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        String name = args.tableDesc.getName();
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 9);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            for (int i = 0; i < 1000; i++) {
                data.put("id", 10L + i);
                data.put("name", "n" + i);
                data.put("sex", true);
                data.put("attribute", "a" + i);
                data.put("image", StringToBB(String.format("i%d", i)));
                data.put("memory", 10 + i);
                data.put("price", 11.1 + i);
                data.put("attribute2", new Date(2020, 5, 2));
                data.put("ts", new DateTime(1588756535));
                Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
                data.clear();
            }
            ReadOption ro = new ReadOption(null, null, null, 0);
            //traverse
            RelationalIterator trit = tableSyncClient.traverse(name, ro);
            for (int i = 0; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 9);
                Assert.assertEquals(TraverseMap.get("id"), 10L + i);
                Assert.assertEquals(TraverseMap.get("name"), "n" + i);
                Assert.assertEquals(TraverseMap.get("sex"), true);
                Assert.assertEquals(TraverseMap.get("attribute"), "a" + i);
                Assert.assertTrue(StringToBB(String.format("i%d", i)).equals(((BlobData) TraverseMap.get("image")).getData()));
                Assert.assertEquals(TraverseMap.get("memory"), 10 + i);
                Assert.assertEquals(TraverseMap.get("price"), 11.1 + i);
                Assert.assertEquals(TraverseMap.get("attribute2"), new Date(2020, 5, 2));
                Assert.assertEquals(TraverseMap.get("ts"), new DateTime(1588756535));
                trit.next();
            }
            Assert.assertEquals(trit.getCount(), 1000);
            Assert.assertFalse(trit.valid());

            // traverse according to pk
            Map<String, Object> index = new HashMap<>();
            index.put("id", 110l);
            index.put("name", "n100");
            index.put("sex", true);
            ro = new ReadOption(index, null, null, 0);
            trit = tableSyncClient.traverse(name, ro);
            for (int i = 100; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 9);
                Assert.assertEquals(TraverseMap.get("id"), 10L + i);
                Assert.assertEquals(TraverseMap.get("name"), "n" + i);
                Assert.assertEquals(TraverseMap.get("sex"), true);
                Assert.assertEquals(TraverseMap.get("attribute"), "a" + i);
                Assert.assertTrue(StringToBB(String.format("i%d", i)).equals(((BlobData) TraverseMap.get("image")).getData()));
                Assert.assertEquals(TraverseMap.get("memory"), 10 + i);
                Assert.assertEquals(TraverseMap.get("price"), 11.1 + i);
                Assert.assertEquals(TraverseMap.get("attribute2"), new Date(2020, 5, 2));
                Assert.assertEquals(TraverseMap.get("ts"), new DateTime(1588756535));
                trit.next();
            }
            Assert.assertEquals(trit.getCount(), 900);
            Assert.assertFalse(trit.valid());

            // traverse failed
            index = new HashMap<>();
            index.put("attribute", "a1");
            index.put("attribute2", new Date(2020, 5, 1));
            ro = new ReadOption(index, null, null, 0);
            try {
                tableSyncClient.traverse(name, ro);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableTraverse() {
        String name = "";
        try {
            name = createRelationalTable(IndexType.PrimaryKey);
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            for (long i = 0; i < 1000; i++) {
                data.put("id", i);
                data.put("attribute", "a" + i);
                data.put("image", StringToBB("i" + i));
                Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
                data.clear();
            }

            Set<String> colSet = new HashSet<>();
            colSet.add("id");
            colSet.add("image");
            ReadOption ro = new ReadOption(null, null, colSet, 0);

            //traverse
            RelationalIterator trit = tableSyncClient.traverse(name, ro);
            for (long i = 0; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 2);
                Assert.assertEquals(TraverseMap.get("id"), i);
                Assert.assertEquals(StringToBB("i" + i), ((BlobData) TraverseMap.get("image")).getData());
                trit.next();
            }
            Assert.assertEquals(trit.getCount(), 1000);
            Assert.assertFalse(trit.valid());

            // traverse according to pk
            Map<String, Object> index = new HashMap<>();
            index.put("id", 100l);
            ro = new ReadOption(index, null, null, 0);
            trit = tableSyncClient.traverse(name, ro);
            for (long i = 100; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 3);
                Assert.assertEquals(TraverseMap.get("id"), i);
                Assert.assertEquals(StringToBB("i" + i), ((BlobData) TraverseMap.get("image")).getData());
                trit.next();
            }
            Assert.assertEquals(trit.getCount(), 900);
            Assert.assertFalse(trit.valid());

            // traverse failed
            index = new HashMap<>();
            index.put("attribute", "a1");
            ro = new ReadOption(index, null, null, 0);
            try {
                tableSyncClient.traverse(name, ro);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableTraverseVarcharKey() {
        String name = createRelationalTable(DataType.Varchar);
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            for (long i = 0; i < 1000; i++) {
                data.put("id", String.format("%04d", i));
                data.put("attribute", "a" + i);
                data.put("image", "i" + i);
                Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
                data.clear();
            }

            Set<String> colSet = new HashSet<>();
            colSet.add("id");
            colSet.add("image");
            ReadOption ro = new ReadOption(null, null, colSet, 1);

            //traverse
            RelationalIterator trit = tableSyncClient.traverse(name, ro);
            for (long i = 0; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 2);
                Assert.assertEquals(TraverseMap.get("id"), String.format("%04d", i));
                Assert.assertEquals(TraverseMap.get("image"), "i" + i);
                trit.next();
            }
            Assert.assertFalse(trit.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableTraverseStringKey() {
        String name = createRelationalTable(DataType.String);
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            for (long i = 1; i < 1000; i++) {
                data.put("id", String.format("%04d", i));
                data.put("attribute", "a" + i);
                data.put("image", "i" + i);
                Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
                data.clear();
            }

            Set<String> colSet = new HashSet<>();
            colSet.add("id");
            colSet.add("image");
            ReadOption ro = new ReadOption(null, null, colSet, 1);

            //traverse
            RelationalIterator trit = tableSyncClient.traverse(name, ro);
            //update key
            UpdateResult updateResult;
            {
                data.clear();
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", "0110");
                data.put("attribute", "aup1110");
                data.put("image", "iup1110");
                updateResult = tableSyncClient.update(name, conditionColumns, data, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                ro = new ReadOption(conditionColumns, null, null, 1);
                RelationalIterator it = tableSyncClient.query(name, ro);
                Map<String, Object> valueMap = new HashMap<>();
                valueMap = it.getDecodedValue();
                Assert.assertEquals(valueMap.size(), 3);
                Assert.assertEquals(valueMap.get("id"), "0110");
                Assert.assertEquals(valueMap.get("attribute"), "aup1110");
                Assert.assertEquals(valueMap.get("image"), "iup1110");
            }
            for (long i = 1; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 2);
                Assert.assertEquals(TraverseMap.get("id"), String.format("%04d", i));
                Assert.assertEquals(TraverseMap.get("image"), "i" + i);
                trit.next();
            }
            Assert.assertEquals(trit.getCount(), 999);
            Assert.assertFalse(trit.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableBatchQueryVarcharKey() {
        String name = createRelationalTable(DataType.String);
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            for (long i = 0; i < 1000; i++) {
                data.put("id", String.format("%04d", i));
                data.put("attribute", "a" + i);
                data.put("image", "i" + i);
                Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
                data.clear();
            }

            List<ReadOption> ros = new ArrayList<ReadOption>();
            for (int i = 0; i < 1000; i++) {
                Set<String> colSet = new HashSet<>();
                colSet.add("id");
                colSet.add("image");
                Map<String, Object> index = new HashMap<String, Object>();
                index.put("id", String.format("%04d", i));
                ReadOption ro = new ReadOption(index, null, colSet, 1);
                ros.add(ro);
            }

            //traverse
            RelationalIterator trit = tableSyncClient.batchQuery(name, ros);
            Assert.assertEquals(trit.getCount(), 1000);
            for (long i = 0; i < 1000; i++) {
                Assert.assertTrue(trit.valid());
                Map<String, Object> TraverseMap = trit.getDecodedValue();
                Assert.assertEquals(TraverseMap.size(), 2);
                Assert.assertEquals(TraverseMap.get("id"), String.format("%04d", i));
                Assert.assertEquals(TraverseMap.get("image"), "i" + i);
                trit.next();
            }
            Assert.assertFalse(trit.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testAutoGenPk() {
        String name = "";
        try {
            name = createRelationalTable(IndexType.AutoGen);
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", "a1");
            data.put("image", StringToBB("i1"));
            try {
                tableSyncClient.put(name, data, wo);
                Assert.fail();
            } catch (TabletException e) {
                Assert.assertTrue(true);
            }
            data.clear();
            data.put("attribute", "a1");
            data.put("image", StringToBB("i1"));
            PutResult pr = tableSyncClient.put(name, data, wo);
            Assert.assertTrue(pr.isSuccess());

            //traverse
            ReadOption ro = new ReadOption(null, null, null, 1);
            RelationalIterator it = tableSyncClient.traverse(name, ro);
            Assert.assertTrue(it.valid());
            Map<String, Object> map = it.getDecodedValue();
            Assert.assertEquals(map.size(), 3);
            Assert.assertEquals(map.get("id"), pr.getAutoGenPk());
            Assert.assertEquals(map.get("attribute"), "a1");
            Assert.assertTrue(StringToBB("i1").equals(((BlobData) map.get("image")).getData()));

            //traverse by pk
            Map<String, Object> index = new HashMap<>();
            index.put("id", pr.getAutoGenPk());
            ro = new ReadOption(index, null, null, 1);
            it = tableSyncClient.traverse(name, ro);
            Assert.assertTrue(it.valid());
            map = it.getDecodedValue();
            Assert.assertEquals(map.size(), 3);
            Assert.assertEquals(map.get("id"), pr.getAutoGenPk());
            Assert.assertEquals(map.get("attribute"), "a1");
            Assert.assertTrue(StringToBB("i1").equals(((BlobData) map.get("image")).getData()));

            //update
            UpdateResult updateResult;
            {
                data.clear();
                Map<String, Object> conditionColumns = new HashMap<>();
                conditionColumns.put("id", pr.getAutoGenPk());
                data.put("id", 111l);
                data.put("image", StringToBB("i2"));
                try {
                    tableSyncClient.update(name, conditionColumns, data, wo);
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(true);
                }

                data.clear();
                conditionColumns = new HashMap<>();
                conditionColumns.put("id", pr.getAutoGenPk());
                data.put("attribute", "a2");
                data.put("image", StringToBB("i2"));
                updateResult = tableSyncClient.update(name, conditionColumns, data, wo);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);

                ro = new ReadOption(conditionColumns, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Map<String, Object> valueMap = new HashMap<>();
                valueMap = it.getDecodedValue();
                Assert.assertEquals(valueMap.size(), 3);
                Assert.assertEquals(valueMap.get("id"), pr.getAutoGenPk());
                Assert.assertEquals(valueMap.get("attribute"), "a2");
                Assert.assertEquals(((BlobData) valueMap.get("image")).getData(), StringToBB("i2"));
            }
            //delete
            {
                Map<String, Object> conditionColumns2 = new HashMap<>();
                conditionColumns2.put("id", pr.getAutoGenPk());
                updateResult = tableSyncClient.delete(name, conditionColumns2);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
                ro = new ReadOption(conditionColumns2, null, null, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertFalse(it.valid());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testAutoGenPkNotFirstIdx() {
        String name = "";
        try {
            name = createRelationalTablePkNotFirstIndex();
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("id", 11l);
            data.put("attribute", 12l);
            data.put("image", "i1");
            try {
                tableSyncClient.put(name, data, wo);
                Assert.fail();
            } catch (TabletException e) {
                Assert.assertTrue(true);
            }
            data.clear();
            data.put("attribute", 12l);
            data.put("image", StringToBB("i1"));
            PutResult pr = tableSyncClient.put(name, data, wo);
            Assert.assertTrue(pr.isSuccess());

            //traverse
            ReadOption ro = new ReadOption(null, null, null, 1);
            RelationalIterator it = tableSyncClient.traverse(name, ro);
            Assert.assertTrue(it.valid());
            Map<String, Object> map = it.getDecodedValue();
            Assert.assertEquals(map.size(), 3);
            Assert.assertEquals(map.get("id"), pr.getAutoGenPk());
            Assert.assertEquals(map.get("attribute"), 12l);
            Assert.assertEquals(((BlobData) map.get("image")).getData(), StringToBB("i1"));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testRelationalTableRecordCnt() {
        String name = createRelationalTable(DataType.Varchar);
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);
            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            for (int i = 0; i < 10; i++) {
                data.put("id", String.format("%d", i));
                data.put("attribute", "a" + i);
                data.put("image", "i" + i);
                Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());
                data.clear();
            }
            Thread.currentThread().sleep(3000);
            TableInfo tableInfo = nsc.showTable(name).get(0);
            long countA[] = new long[1];
            long countB[] = new long[1];
            getRecordCount(tableInfo, countA, countB);
            Assert.assertEquals(countA[0], 10);
            Assert.assertEquals(countA[0], countB[0]);

            // delete
            for (int i = 0; i < 3; i++) {
                Map<String, Object> idxMap = new HashMap<>();
                idxMap.put("id", String.format("%d", i));
                UpdateResult updateResult = tableSyncClient.delete(name, idxMap);
                Assert.assertTrue(updateResult.isSuccess());
                Assert.assertEquals(updateResult.getAffectedCount(), 1);
            }
            Thread.currentThread().sleep(3000);
            countA[0] = 0;
            countB[0] = 0;
            tableInfo = nsc.showTable(name).get(0);
            getRecordCount(tableInfo, countA, countB);
            Assert.assertEquals(countA[0], 7);
            Assert.assertEquals(countA[0], countB[0]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    private void getRecordCount(TableInfo tableInfo, long[] countA, long[] countB) {
        for (int i = 0; i < tableInfo.getTablePartitionList().size(); i++) {
            TablePartition tablePartition = tableInfo.getTablePartition(i);
            countA[0] += tablePartition.getRecordCnt();
            for (int j = 0; j < tablePartition.getPartitionMetaList().size(); j++) {
                PartitionMeta partitionMeta = tablePartition.getPartitionMeta(j);
                if (partitionMeta.getIsAlive() && partitionMeta.getIsLeader()) {
                    countB[0] += partitionMeta.getRecordCnt();
                }
            }
        }
    }

    @Test
    public void testAddTableFieldWithColumnKey() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts_1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts_1").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setPartitionNum(1).setReplicaNum(1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 5);
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            data.put("ts", 1234l);
            data.put("ts_1", 222l);
            tableSyncClient.put(name, data);

            KvIterator itt = tableSyncClient.traverse(name, "card");
            Assert.assertTrue(itt.valid());
            Assert.assertEquals(itt.getSchema().size(), 5);

            ok = nsc.addTableField(name, "aa", "string");
//            Thread.currentThread().sleep(15);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 6);

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            data.put("ts_1", 333l);
            data.put("aa", "aa1");
            tableSyncClient.put(name, data);

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.7);
            data.put("ts", 1236l);
            data.put("ts_1", 444l);
            tableSyncClient.put(name, data);

            Object[] row = tableSyncClient.getRow(name, "card0", "card", 1236l, "ts", null);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7);
            Assert.assertEquals(row[3], 1236l);
            Assert.assertEquals(row[4], 444l);
            Assert.assertEquals(row[5], null);

            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 2);
            Assert.assertEquals(it.getSchema().size(), 6);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 1235);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1235l);
            Assert.assertEquals(((Long) row[4]).longValue(), 333l);
            Assert.assertEquals(row[5], "aa1");
            it = tableSyncClient.scan(name, "card0", "card", 1235l, 0l, "ts_1", 0);
            Assert.assertEquals(it.getCount(), 3);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 444);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1236l);
            Assert.assertEquals(((Long) row[4]).longValue(), 444l);
            Assert.assertEquals(row[5], null);
            it = tableSyncClient.scan(name, "mcc1", "mcc", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);

            itt = tableSyncClient.traverse(name, "card");
            Assert.assertTrue(itt.valid());
            Assert.assertEquals(it.getSchema().size(), 6);
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 1236);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1236l);
            Assert.assertEquals(((Long) row[4]).longValue(), 444l);
            Assert.assertEquals(row[5], null);

            itt.next();
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 1235);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1235l);
            Assert.assertEquals(((Long) row[4]).longValue(), 333l);
            Assert.assertEquals(row[5], "aa1");

            itt.next();
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 1234);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 1.5d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1234l);
            Assert.assertEquals(((Long) row[4]).longValue(), 222l);
            Assert.assertEquals(row[5], null);

            ok = nsc.addTableField(name, "bb", "string");
//            Thread.currentThread().sleep(15);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 7);
            itt = tableSyncClient.traverse(name, "card");
            Assert.assertTrue(itt.valid());
            Assert.assertEquals(itt.getSchema().size(), 7);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testAddTableFieldWithoutColumnKey() {
        String name = createSchemaTable();
        try {
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 3);
            Assert.assertTrue(tableSyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d}));

            boolean ok = nsc.addTableField(name, "aa", "string");
//            Thread.currentThread().sleep(15);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 4);

            Assert.assertTrue(tableSyncClient.put(name, 9528, new Object[]{"card1", "mcc1", 9.2d, "aa1"}));
            Assert.assertTrue(tableSyncClient.put(name, 9529, new Object[]{"card2", "mcc2", 9.3d}));

            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            Assert.assertEquals(row[3], null);

            row = tableSyncClient.getRow(name, "card1", 9528);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
            Assert.assertEquals(row[3], "aa1");

            row = tableSyncClient.getRow(name, "card2", 9529);
            Assert.assertEquals(row[0], "card2");
            Assert.assertEquals(row[1], "mcc2");
            Assert.assertEquals(row[2], 9.3d);
            Assert.assertEquals(row[3], null);

            Assert.assertTrue(tableSyncClient.put(name, 9528, new Object[]{"card0", "mcc1", 9.2d, "aa1"}));
            Assert.assertTrue(tableSyncClient.put(name, 9529, new Object[]{"card0", "mcc2", 9.3d}));

            KvIterator it = tableSyncClient.scan(name, "card0", "card", 9530, 0);
            Assert.assertEquals(it.getCount(), 3);
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc2", row[1]);
            Assert.assertEquals(9.3d, row[2]);
            Assert.assertEquals(null, row[3]);

            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc1", row[1]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals("aa1", row[3]);

            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc0", row[1]);
            Assert.assertEquals(9.15d, row[2]);
            Assert.assertEquals(null, row[3]);

            KvIterator itt = tableSyncClient.traverse(name, "card");
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 9528);
            Assert.assertEquals(itt.getPK(), "card1");
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);

            itt.next();
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 9529);
            Assert.assertEquals(itt.getPK(), "card0");
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc2");
            Assert.assertEquals(row[2], 9.3d);
            Assert.assertEquals(row[3], null);

            itt.next();
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 9528);
            Assert.assertEquals(itt.getPK(), "card0");
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
            Assert.assertEquals(row[3], "aa1");

            itt.next();
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 9527);
            Assert.assertEquals(itt.getPK(), "card0");
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            Assert.assertEquals(row[3], null);

            itt.next();
            Assert.assertTrue(itt.valid());
            row = itt.getDecodedValue();
            Assert.assertEquals(itt.getKey(), 9529);
            Assert.assertEquals(itt.getPK(), "card2");
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card2");
            Assert.assertEquals(row[1], "mcc2");
            Assert.assertEquals(row[2], 9.3d);
            Assert.assertEquals(row[3], null);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testCodecForAddTableFiled() {
        String name = createSchemaTable();
        try {
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 3);
            Assert.assertTrue(tableSyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d}));

            boolean ok = nsc.addTableField(name, "aa", "string");
//            Thread.currentThread().sleep(15);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            Assert.assertEquals(tableSyncClient.getSchema(name).size(), 4);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = new ArrayList<com._4paradigm.rtidb.client.schema.ColumnDesc>();
        com._4paradigm.rtidb.client.schema.ColumnDesc col1 = new com._4paradigm.rtidb.client.schema.ColumnDesc();
        col1.setAddTsIndex(true);
        col1.setName("card");
        col1.setType(ColumnType.kString);
        schema.add(col1);

        com._4paradigm.rtidb.client.schema.ColumnDesc col2 = new com._4paradigm.rtidb.client.schema.ColumnDesc();
        col2.setAddTsIndex(true);
        col2.setName("mcc");
        col2.setType(ColumnType.kString);
        schema.add(col2);

        com._4paradigm.rtidb.client.schema.ColumnDesc col3 = new com._4paradigm.rtidb.client.schema.ColumnDesc();
        col3.setAddTsIndex(false);
        col3.setName("amt");
        col3.setType(ColumnType.kDouble);
        schema.add(col3);

        com._4paradigm.rtidb.client.schema.ColumnDesc col4 = new com._4paradigm.rtidb.client.schema.ColumnDesc();
        col4.setAddTsIndex(false);
        col4.setName("aa");
        col4.setType(ColumnType.kString);
        schema.add(col4);
        try {
            ByteBuffer buffer = RowCodec.encode(new Object[]{"9527", "1234", 1.0, "aa1"}, schema);
            buffer.rewind();

            Object[] row = new Object[4];
            RowCodec.decode(buffer, schema, row, 0, 4);
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals("1234", row[1]);
            Assert.assertEquals(1.0, row[2]);
            Assert.assertEquals("aa1", row[3]);

            buffer.rewind();
            row = new Object[3];
            schema = schema.subList(0, 2);
            RowCodec.decode(buffer, schema, row, 0, 3);
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals("1234", row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (TabletException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testTraverse() {
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            Thread.sleep(200);
            KvIterator it = tableSyncClient.traverse(name, "card");
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 9528);
            Assert.assertEquals(it.getPK(), "card0");
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 9527);
            Assert.assertEquals(it.getPK(), "card0");
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            it.next();
            Assert.assertFalse(it.valid());
            for (int i = 0; i < 200; i++) {
                rowMap = new HashMap<String, Object>();
                rowMap.put("card", "card" + (i + 9529));
                rowMap.put("mcc", "mcc" + i);
                rowMap.put("amt", 9.2d);
                ok = tableSyncClient.put(name, i + 9529, rowMap);
                Assert.assertTrue(ok);
            }
            it = tableSyncClient.traverse(name, "card");
            for (int j = 0; j < 202; j++) {
                Assert.assertTrue(it.valid());
                it.next();
            }
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testScanLimit() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 9529, 1000, 2);
            Assert.assertTrue(it.getCount() == 2);
            Assert.assertTrue(it.valid());
            byte[] buffer = new byte[6];
            it.getValue().get(buffer);
            String value = new String(buffer);
            Assert.assertEquals(value, "value2");
            it.next();

            Assert.assertTrue(it.valid());
            it.getValue().get(buffer);
            value = new String(buffer);
            Assert.assertEquals(value, "value1");
            it.next();
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testScanLatestN() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 2);
            Assert.assertTrue(it.getCount() == 2);
            Assert.assertTrue(it.valid());
            byte[] buffer = new byte[6];
            it.getValue().get(buffer);
            String value = new String(buffer);
            Assert.assertEquals(value, "value2");
            it.next();

            Assert.assertTrue(it.valid());
            it.getValue().get(buffer);
            value = new String(buffer);
            Assert.assertEquals(value, "value1");
            it.next();
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testSchemaPutForMap() {

        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            Thread.sleep(200);
            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            row = tableSyncClient.getRow(name, "card1", 9528);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testNullDimension() {
        String name = createSchemaTable();
        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[]{null, "1222", 1.0});
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "1222", "mcc", 12, 9);
            Assert.assertNotNull(it);
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(null, row[0]);
            Assert.assertEquals("1222", row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[]{"9527", null, 1.0});
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "9527", "card", 12, 9);
            Assert.assertNotNull(it);
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals(null, row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            tableSyncClient.put(name, 10, new Object[]{null, null, 1.0});
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        try {
            tableSyncClient.put(name, 10, new Object[]{"", "", 1.0});
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testScanDuplicateRecord() {
        ClientBuilder.config.setRemoveDuplicateByTime(true);
        String name = createSchemaTable();
        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 10, new Object[]{"card0", "1223", 2.0});
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 12, 9);
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("1223", row[1]);
            Assert.assertEquals(2.0, row[2]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            ClientBuilder.config.setRemoveDuplicateByTime(false);
        }

    }

    @Test
    public void testGetWithOperator() {
        String name = createSchemaTable("kLatestTime");
        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 11, new Object[]{"card0", "1224", 2.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 13, new Object[]{"card0", "1224", 3.0});
            Assert.assertTrue(ok);
            // equal
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 13, Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // le
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 11, Tablet.GetType.kSubKeyLe);
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }

            // ge
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 12, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // ge
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 13, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // gt
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 12, Tablet.GetType.kSubKeyGt);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // gt
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 11, Tablet.GetType.kSubKeyGt);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }
            // le
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 12, Tablet.GetType.kSubKeyLe);
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testIsRunning() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().setTtlType("kLatestTime").addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(10)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        System.out.println(table);
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        System.out.println(name);
        try {
            ok = tableSyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 9528, new Object[]{"card1", "mcc1", 9.2d});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            row = tableSyncClient.getRow(name, "card1", 9528);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
//        return name;
    }


    @Test
    public void testGetWithOpDefault() {
        String name = createSchemaTable("kLatestTime");
        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 11, new Object[]{"card0", "1224", 2.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 13, new Object[]{"card0", "1224", 3.0});
            Assert.assertTrue(ok);
            // range
            {
                Object[] row = tableSyncClient.getRow(name, "card0", "card", 14, null, Tablet.GetType.kSubKeyLe,
                        9, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            //
            {
                Object[] row = tableSyncClient.getRow(name, "card0", "card", 14, null, Tablet.GetType.kSubKeyLe,
                        14, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(null, row);
            }

            //
            {
                Object[] row = tableSyncClient.getRow(name, "card0", "card", 13, null, Tablet.GetType.kSubKeyEq,
                        13, Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            {
                Object[] row = tableSyncClient.getRow(name, "card0", "card", 11, null, Tablet.GetType.kSubKeyEq,
                        11, Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSchemaPutByKvWay() {

        String name = createSchemaTable();
        try {
            tableSyncClient.put(name, "11", 1535371622000l, "11");
            Assert.assertTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testCountSchemaTable() {
        String name = createSchemaTable();
        try {
            String k1 = "k1";
            String k2 = "k2";
            for (int i = 1; i < 10; i++) {
                boolean ok = tableSyncClient.put(name, i, new Object[]{k1, k2, 1.0});
                Assert.assertTrue(ok);
            }
            int count = tableSyncClient.count(name, k1, "card", 10, 9);
            Assert.assertEquals(0, count);
            count = tableSyncClient.count(name, k1, "card", null, 10, 9);
            Assert.assertEquals(0, count);
            count = tableSyncClient.count(name, k1, "card", 10, 8);
            Assert.assertEquals(1, count);
            count = tableSyncClient.count(name, k1, "card", null, 10, 8);
            Assert.assertEquals(1, count);
            count = tableSyncClient.count(name, k1, "card", null, 10, 7);
            Assert.assertEquals(2, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            int cnt = tableSyncClient.count(name, "k1", "card", null, 7, 10);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testNoTsPut() throws TabletException, ExecutionException, InterruptedException, TimeoutException {
        String name = createSchemaTable();
        Map<String, Object> row = new HashMap<>();
        row.put("card", "card1");
        row.put("mcc", "cc1");
        row.put("amt", 1.0d);
        boolean ok = tableSyncClient.put(name, row);
        Assert.assertTrue(ok);
        ok = tableSyncClient.put(name, new Object[]{"card2", "cc2", 2.0d});
        Assert.assertTrue(ok);
    }

    @Test
    public void testCountKvTable() {
        String name = createKvTable();
        try {
            String key = "k1";
            for (int i = 1; i < 10; i++) {
                boolean ok = tableSyncClient.put(name, key, i, String.valueOf(i));
                Assert.assertTrue(ok);
            }
            int count = tableSyncClient.count(name, key, 10, 9);
            Assert.assertEquals(0, count);
            count = tableSyncClient.count(name, key, null, null, 10, 9);
            Assert.assertEquals(0, count);
            Assert.assertEquals(1, tableSyncClient.count(name, key, 10, 8));
            count = tableSyncClient.count(name, key, null, null, 10, 8);
            Assert.assertEquals(1, count);
            count = tableSyncClient.count(name, key, null, null, 10, 7);
            Assert.assertEquals(2, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTraverSeEmptyKvTest() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableInfo tableinfo = TableInfo.newBuilder().setName(name).setSegCnt(8).setReplicaNum(1).setTtl(0).build();
        boolean ok = nsc.createTable(tableinfo);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            long basets = 1564992840;
            String[] value = {"test", "", "test1", ""};

            for (int i = 0; i < value.length; i++) {
                ok = tableSyncClient.put(name, "key1", basets + i, value[i]);
                Assert.assertTrue(ok);
                Assert.assertEquals(i + 1, tableSyncClient.count(name, "key1"));
            }

            KvIterator it = tableSyncClient.traverse(name);
            Assert.assertTrue(it.valid());
            for (int i = value.length - 1; i > 0; i--) {
                byte[] buffer = new byte[it.getValue().remaining()];
                it.getValue().get(buffer);
                String v = new String(buffer);
                Assert.assertEquals(value[i], v);
                Assert.assertEquals(it.getKey(), basets + i);
                Assert.assertEquals(it.getPK(), "key1");
                it.next();
                Assert.assertTrue(it.valid());
            }
            it = tableSyncClient.traverse(name);
            for (int i = 0; i < value.length; i++) {
                Assert.assertTrue(it.valid());
                it.next();
            }
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }

    }
}
