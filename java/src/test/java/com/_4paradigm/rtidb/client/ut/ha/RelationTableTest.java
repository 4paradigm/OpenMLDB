package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.Config;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.impl.RelationalIterator;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.IndexDef;
import com._4paradigm.rtidb.client.schema.TableDesc;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.client.type.IndexType;
import com._4paradigm.rtidb.client.type.TableType;
import com._4paradigm.rtidb.ns.NS;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RelationTableTest extends TestCaseBase {

    private static AtomicInteger id = new AtomicInteger(90000);
    private static String[] nodes = Config.NODES;
    private final static Logger logger = LoggerFactory.getLogger(ColumnKeyTest.class);

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    private String createRelationalTable(IndexType indexType) throws TabletException {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<ColumnDesc> list = new ArrayList<>();
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

    private String createRelationalTableStringPk() throws TabletException {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableDesc tableDesc = new TableDesc();
        tableDesc.setName(name);
        tableDesc.setTableType(TableType.kRelational);
        List<com._4paradigm.rtidb.client.schema.ColumnDesc> list = new ArrayList<>();
        {
            com._4paradigm.rtidb.client.schema.ColumnDesc col = new com._4paradigm.rtidb.client.schema.ColumnDesc();
            col.setName("desc");
            col.setDataType(DataType.String);
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
        colNameList.add("desc");
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

    private void getRecordCount(NS.TableInfo tableInfo, long[] countA, long[] countB) {
        for (int i = 0; i < tableInfo.getTablePartitionList().size(); i++) {
            NS.TablePartition tablePartition = tableInfo.getTablePartition(i);
            countA[0] += tablePartition.getRecordCnt();
            for (int j = 0; j < tablePartition.getPartitionMetaList().size(); j++) {
                NS.PartitionMeta partitionMeta = tablePartition.getPartitionMeta(j);
                if (partitionMeta.getIsAlive() && partitionMeta.getIsLeader()) {
                    countB[0] += partitionMeta.getRecordCnt();
                }
            }
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
    public void testRelationTableChineseString() {
        String name = "";
        try {
            name = createRelationalTableStringPk();
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 3);

            //put
            WriteOption wo = new WriteOption();
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("desc", "赵钱孙李zqsl");
            data.put("attribute", "zqsl");
            String imageData1 = "zqsl";
            ByteBuffer buf1 = StringToBB(imageData1);
            data.put("image", buf1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            data.clear();
            data.put("desc", "zwzw");
            data.put("attribute", "zwzw");
            String imageData2 = "zwzw";
            ByteBuffer buf2 = StringToBB(imageData2);
            data.put("image", buf2);
            tableSyncClient.put(name, data, wo);

            data.clear();
            data.put("desc", "冯陈诸卫（fczw");
            data.put("attribute", "fczw");
            String imageData3 = "fczw";
            ByteBuffer buf3 = StringToBB(imageData3);
            data.put("image", buf3);
            tableSyncClient.put(name, data, wo);
            ReadOption ro;
            RelationalIterator it;
            Map<String, Object> queryMap;
            //query
            {
                Map<String, Object> index = new HashMap<>();
                index.put("desc", "赵钱孙李zqsl");
                Set<String> colSet = new HashSet<>();
                colSet.add("desc");
                colSet.add("image");
                ro = new ReadOption(index, null, colSet, 1);
                it = tableSyncClient.query(name, ro);
                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 2);
                Assert.assertEquals(queryMap.get("desc"), "赵钱孙李zqsl");
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));
            }
            //batch query
            {
                List<ReadOption> ros = new ArrayList<ReadOption>();
                {
                    Map<String, Object> index = new HashMap<>();
                    index.put("desc", "赵钱孙李zqsl");
                    Set<String> colSet = new HashSet<>();
                    colSet.add("desc");
                    colSet.add("attribute");
                    colSet.add("image");
                    ro = new ReadOption(index, null, colSet, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("desc", "zwzw");
                    Set<String> colSet = new HashSet<>();
                    colSet.add("desc");
                    colSet.add("attribute");
                    colSet.add("image");
                    ro = new ReadOption(index2, null, colSet, 1);
                    ros.add(ro);
                }
                {
                    Map<String, Object> index2 = new HashMap<>();
                    index2.put("desc", "冯陈诸卫（fczw");
                    Set<String> colSet = new HashSet<>();
                    colSet.add("desc");
                    colSet.add("attribute");
                    colSet.add("image");
                    ro = new ReadOption(index2, null, colSet, 1);
                    ros.add(ro);
                }
                it = tableSyncClient.batchQuery(name, ros);
                Assert.assertEquals(it.getCount(), 3);

                Assert.assertTrue(it.valid());
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("desc"), "赵钱孙李zqsl");
                Assert.assertEquals(queryMap.get("attribute"), "zqsl");
                Assert.assertTrue(buf1.equals(((BlobData) queryMap.get("image")).getData()));

                it.next();
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("desc"), "zwzw");
                Assert.assertEquals(queryMap.get("attribute"), "zwzw");
                Assert.assertTrue(buf2.equals(((BlobData) queryMap.get("image")).getData()));
                it.next();
                queryMap = it.getDecodedValue();
                Assert.assertEquals(queryMap.size(), 3);
                Assert.assertEquals(queryMap.get("desc"), "冯陈诸卫（fczw");
                Assert.assertEquals(queryMap.get("attribute"), "fczw");
                Assert.assertTrue(buf3.equals(((BlobData) queryMap.get("image")).getData()));
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
            Assert.assertEquals((ByteBuffer) data.get("image"), buf1);

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
            data.clear();
            data.put("id", 12l);
            data.put("attribute", "a2");
            imageData2 = "i1";
            buf2 = StringToBB(imageData2);
            data.put("image", buf2);
            Assert.assertTrue(tableSyncClient.put(name, data, new WriteOption(true)).isSuccess());
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
            data.put("attribute", "a0");
            ByteBuffer buf1 = StringToBB("i1");
            data.put("image", buf1);
            data.put("memory", 11);
            data.put("price", 11.1);
            Assert.assertTrue(tableSyncClient.put(name, data, wo).isSuccess());

            Assert.assertEquals((ByteBuffer) data.get("image"), buf1);
            data.put("attribute", "a1");
            Assert.assertTrue(tableSyncClient.put(name, data, new WriteOption(true)).isSuccess());

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
                Assert.assertEquals((ByteBuffer) valueColumns.get("image"), buf3);

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
                valueColumns.put("memory", 12);
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
        list.add(new HashMap<String, Object>() { { put("id", 12l);} });
        list.add(new HashMap<String, Object>() { { put("price", 11.1);} });
        list.add(new HashMap<String, Object>() { { put("id", 11l);} });
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
            data.put("attribute2", new java.sql.Date(2020, 5, 1));
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
            data.put("attribute2", new java.sql.Date(2020, 5, 2));
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
            data.put("attribute2", new java.sql.Date(2020, 5, 3));
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
            data.put("attribute2", new java.sql.Date(2020, 5, 4));
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
            conditionColumns.put("attribute2", new java.sql.Date(2020, 5, 1));
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
            conditionColumns.put("attribute2", new java.sql.Date(2020, 5, 1));
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
            Map map = new HashMap((Map) (args.row[0]));
            map.put("attribute", "a0");
            Assert.assertTrue(tableSyncClient.put(name, map, wo).isSuccess());
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[i]), new WriteOption(true)).isSuccess());
            }

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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 2));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 3));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 2));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 3));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 2));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 3));
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
        if (!ok) {
            logger.warn("xxxx error");
        }
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        String name = args.tableDesc.getName();
        try {
            List<com._4paradigm.rtidb.client.schema.ColumnDesc> schema = tableSyncClient.getSchema(name);
            Assert.assertEquals(schema.size(), 9);
            //put
            WriteOption wo = new WriteOption();
            for (int i = 0; i < 4; i ++) {
                Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[i]), wo).isSuccess());
            }
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 2));
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 3));
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

    @Test(dataProvider = "relational_combine_key_case")
    public void testRelationalTablePutCover(RelationTestArgs args) {
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
            Map map = new HashMap((Map) (args.row[0]));
            map.put("attribute", "a0");
            map.put("image", null);
            Assert.assertTrue(tableSyncClient.put(name, map, wo).isSuccess());
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
                Assert.assertEquals(queryMap.get("attribute"), "a0");
                Assert.assertEquals(queryMap.get("image"),null);
                Assert.assertEquals(queryMap.get("memory"), 11);
                Assert.assertEquals(queryMap.get("price"), 11.1);
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
            }
            Assert.assertTrue(tableSyncClient.put(name, (Map) (args.row[0]), new WriteOption(true)).isSuccess());
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
                Assert.assertEquals(queryMap.get("attribute2"), new java.sql.Date(2020, 5, 1));
                Assert.assertEquals(queryMap.get("ts"), new DateTime(1588756531));
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
                NS.TableInfo tableInfo = nsc.showTable(name).get(0);
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
                data.put("attribute2", new java.sql.Date(2020, 5, 2));
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
                Assert.assertEquals(TraverseMap.get("attribute2"), new java.sql.Date(2020, 5, 2));
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
                Assert.assertEquals(TraverseMap.get("attribute2"), new java.sql.Date(2020, 5, 2));
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
            NS.TableInfo tableInfo = nsc.showTable(name).get(0);
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

}
