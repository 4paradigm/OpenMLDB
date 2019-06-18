package com._4paradigm.rtidb.client.ut;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;

import io.brpc.client.EndPoint;

public class TabletSchemaClientTest  extends TestCaseBase {

    private final static AtomicInteger id = new AtomicInteger(8000);
    private static TabletClientImpl tabletClient = null;
    private static TableSyncClient tableClient = null;

    @BeforeClass
    public  void setUp() {
        super.setUp();
        tabletClient = super.tabletClient;
        tableClient = super.tableSingleNodeSyncClient;
    }
    
    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testEmptyTableNameCreate() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card1");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("card2");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        boolean ok = tabletClient.createTable("", tid, 0, 0, 8, schema);
        Assert.assertFalse(ok);
    }

    @Test
    public void testLatestTtlCreate() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card1");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("card2");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        boolean ok = tabletClient.createTable("latest ttl", tid, 0, 0, TTLType.kLatestTime, 8, schema);
        Assert.assertTrue(ok);
    }

    @Test
    public void testTTL() {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.createTable("ttl", tid, 0, 1000, TTLType.kLatestTime, 8);
        Assert.assertTrue(ok);
        Assert.assertTrue(tabletClient.dropTable(tid, 0));
        tid = id.incrementAndGet();
        ok = tabletClient.createTable("ttl1", tid, 0, 1001, TTLType.kLatestTime, 8);
        Assert.assertFalse(ok);
        int max_ttl = 60*24*365*30;
        tid = id.incrementAndGet();
        ok = tabletClient.createTable("ttl2", tid, 0, max_ttl + 1, TTLType.kAbsoluteTime, 8);
        Assert.assertFalse(ok);
        tid = id.incrementAndGet();
        ok = tabletClient.createTable("ttl3", tid, 0, max_ttl, TTLType.kAbsoluteTime, 8);
        Assert.assertTrue(ok);
        Assert.assertTrue(tabletClient.dropTable(tid, 0));
    }

    @Test
    public void testEmptyColNameCreate() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("card");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertFalse(ok);
    }

    @Test
    public void testNullNameCreate() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName(null);
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("card");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertFalse(ok);
    }

    @Test
    public void testDuplicatedCreate() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("card");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertFalse(ok);
    }

    @Test
    public void testEmptyTTLLatestCreate() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card1");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("card2");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 2, TTLType.kLatestTime, 8, schema);
        Assert.assertTrue(ok);
    }

    @Test
    public void test0Create() {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        TableHandler th = snc.getHandler(tid);
        Assert.assertTrue(th.getIndexes().size() == 2);
        Assert.assertTrue(th.getSchema().size() == 3);

        Assert.assertEquals(true, th.getSchema().get(0).isAddTsIndex());
        Assert.assertEquals("card", th.getSchema().get(0).getName());
        Assert.assertEquals(ColumnType.kString, th.getSchema().get(0).getType());

        Assert.assertEquals(true, th.getSchema().get(1).isAddTsIndex());
        Assert.assertEquals("merchant", th.getSchema().get(1).getName());
        Assert.assertEquals(ColumnType.kString, th.getSchema().get(1).getType());

        Assert.assertEquals(false, th.getSchema().get(2).isAddTsIndex());
        Assert.assertEquals("amt", th.getSchema().get(2).getName());
        Assert.assertEquals(ColumnType.kDouble, th.getSchema().get(2).getType());
        //Assert.assertEquals(th.getIndexes().get("card").intValue(), 0);
        //Assert.assertEquals(th.getIndexes().get("merchant").intValue(), 1);

    }

    @Test
    public void test1Put() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(tableClient.put(tid, 0, 10, new Object[] { "9527", "1222", 1.0 }));
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void test2Scan() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(tableClient.put(tid, 0, 10, new Object[] { "9527", "1222", 1.0 }));
        Assert.assertTrue(tableClient.put(tid, 0, 11, new Object[] { "9527", "1221", 2.0 }));
        Assert.assertTrue(tableClient.put(tid, 0, 12, new Object[] { "9524", "1222", 3.0 }));
        KvIterator it = tableClient.scan(tid, 0, "9527", "card", 12l, 9);
        Assert.assertTrue(it != null);

        Assert.assertTrue(it.valid());
        Object[] row = it.getDecodedValue();
        Assert.assertTrue(row.length == 3);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("1221", row[1]);
        Assert.assertEquals(2.0, row[2]);
        it.next();

        Assert.assertTrue(it.valid());
        row = it.getDecodedValue();
        Assert.assertTrue(row.length == 3);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("1222", row[1]);
        Assert.assertEquals(1.0, row[2]);
        it.next();
        Assert.assertFalse(it.valid());

        it = tableClient.scan(tid, 0, "1222", "merchant", 12l, 9);
        Assert.assertTrue(it != null);

        Assert.assertTrue(it.valid());
        row = it.getDecodedValue();
        Assert.assertTrue(row.length == 3);
        Assert.assertEquals("9524", row[0]);
        Assert.assertEquals("1222", row[1]);
        Assert.assertEquals(3.0, row[2]);
        it.next();

        Assert.assertTrue(it.valid());
        row = it.getDecodedValue();
        Assert.assertTrue(row.length == 3);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("1222", row[1]);
        Assert.assertEquals(1.0, row[2]);
        it.next();
        Assert.assertFalse(it.valid());
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void testBigColumnScan() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        String str128 = new String(new byte[128]);
        Assert.assertTrue(tableClient.put(tid, 0, 10, new Object[] { "9527", str128, 2.0 }));
        KvIterator it = tableClient.scan(tid, 0, "9527", "card", 12l, 9l);
        Assert.assertTrue(it != null);
        Assert.assertTrue(it.valid());
        Object[] row = it.getDecodedValue();
        Assert.assertTrue(row.length == 3);
        Assert.assertEquals(128, row[1].toString().length());
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals(2.0, row[2]);
        String str129 = new String(new byte[129]);
        try {
            tableClient.put(tid, 0, 11, new Object[] { str129, "1221", 2.0 });
            Assert.assertFalse(true);
        } catch (Exception e) {
            Assert.assertFalse(false);
        }
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void testLatestTTLScan() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 1, TTLType.kLatestTime, 8, schema);
        Assert.assertTrue(ok);
        String str128 = new String(new byte[128]);
        Assert.assertTrue(tableClient.put(tid, 0, 10, new Object[] { "9527", str128, 2.0 }));
        Assert.assertTrue(tableClient.put(tid, 0, 11, new Object[] { "9527", str128, 3.0 }));
        // wait two minutes
        try {
            Thread.sleep(1000 * 120);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            tableClient.scan(tid, 0, "9527", "card", 12l, 0l);
            Assert.assertTrue(false);
        }catch(TabletException e) {
            Assert.assertTrue(true);
        }



    }

    @Test
    public void testPutNullAndScan() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(false);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(tableClient.put(tid, 0, 10l, new Object[] { "9527", null, 2.0d }));
        Assert.assertTrue(tableClient.put(tid, 0, 1l, new Object[] { "9527", "test", null }));

        KvIterator it = tableClient.scan(tid, 0, "9527", "card", 12l, 0l);
        Assert.assertTrue(it.valid());

        Object[] row = it.getDecodedValue();
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals(null, row[1]);
        Assert.assertEquals(2.0d, row[2]);
        it.next();

        Assert.assertTrue(it.valid());
        row = it.getDecodedValue();
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("test", row[1]);
        Assert.assertEquals(null, row[2]);
        it.next();
        Assert.assertFalse(it.valid());

    }

    @Test
    public void testSchemaGet() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("schema-get", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(tableClient.put(tid, 0, 10l, new Object[] { "9527", "merchant0", 2.0d }));
        // check no exist
        try {
            Object[] row = tableClient.getRow(tid, 0, "9528", 0l);
        } catch (TabletException e) {
            Assert.assertEquals(109, e.getCode());
        }

        // get head
        Object[] row = tableClient.getRow(tid, 0, "9527", 0l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("merchant0", row[1]);
        Assert.assertEquals(2.0d, row[2]);
        // get by time
        row = tableClient.getRow(tid, 0, "9527", 10l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("merchant0", row[1]);
        Assert.assertEquals(2.0d, row[2]);
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void testMulGet() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("schema-get", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(tableClient.put(tid, 0, 10l, new Object[] { "9527", "merchant0", 2.0d }));
        Assert.assertTrue(tableClient.put(tid, 0, 20l, new Object[] { "9527", "merchant1", 3.0d }));
        Assert.assertTrue(tableClient.put(tid, 0, 30l, new Object[] { "9528", "merchant0", 4.0d }));
        // check no exist
        try {
            Object[] row = tableClient.getRow(tid, 0, "9529", "card", 0l);
        } catch (TabletException e) {
            Assert.assertEquals(109, e.getCode());
        }

        // get head
        Object[] row = tableClient.getRow(tid, 0, "9527", "card",0l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("merchant1", row[1]);
        Assert.assertEquals(3.0d, row[2]);
        // get by time
        row = tableClient.getRow(tid, 0, "9527","card", 10l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("merchant0", row[1]);
        Assert.assertEquals(2.0d, row[2]);

        // get by time
        row = tableClient.getRow(tid, 0, "merchant0", "merchant" ,30l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9528", row[0]);
        Assert.assertEquals("merchant0", row[1]);
        Assert.assertEquals(4.0d, row[2]);
        tabletClient.dropTable(tid, 0);
    }
    
    public void testUtf8() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
        ColumnDesc desc1 = new ColumnDesc();
        desc1.setAddTsIndex(true);
        desc1.setName("card");
        desc1.setType(ColumnType.kString);
        schema.add(desc1);
        ColumnDesc desc2 = new ColumnDesc();
        desc2.setAddTsIndex(true);
        desc2.setName("merchant");
        desc2.setType(ColumnType.kString);
        schema.add(desc2);
        ColumnDesc desc3 = new ColumnDesc();
        desc3.setAddTsIndex(false);
        desc3.setName("amt");
        desc3.setType(ColumnType.kDouble);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("schema-get", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(tableClient.put(tid, 0, 10l, new Object[] { "a", "中文2", 2.0d }));
        Object[] row = tableClient.getRow(tid, 0, "a", 0l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("a", row[0]);
        Assert.assertEquals("中文2", row[1]);
        Assert.assertEquals(2.0d, row[2]);
    }
}
