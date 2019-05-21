package com._4paradigm.rtidb.client.ut;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

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

public class TableSchemaClientTest {

    private final static AtomicInteger id = new AtomicInteger(5000);
    private static TableSyncClientImpl client = null;
    private static TabletClientImpl tabletClient = null;
    private static EndPoint endpoint = new EndPoint(Config.ENDPOINT);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
   
    @BeforeClass
    public static void setUp() {
        try {
            snc.init();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        client = new TableSyncClientImpl(snc);
        tabletClient = new TabletClientImpl(snc);
    }

    @AfterClass
    public static void tearDown() {
        snc.close();
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
        Assert.assertEquals(th.getIndexes().get(0).size(), 1);
        Assert.assertEquals(th.getIndexes().get(1).size(), 1);

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
        Assert.assertTrue(client.put(tid, 0, 10, new Object[] { "9527", "1222", 1.0 }));
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
        Assert.assertTrue(client.put(tid, 0, 10, new Object[] { "9527", "1222", 1.0 }));
        Assert.assertTrue(client.put(tid, 0, 11, new Object[] { "9527", "1221", 2.0 }));
        Assert.assertTrue(client.put(tid, 0, 12, new Object[] { "9524", "1222", 3.0 }));
        KvIterator it = client.scan(tid, 0, "9527", "card", 12l, 9);
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

        it = client.scan(tid, 0, "1222", "merchant", 12l, 9);
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
        Assert.assertTrue(client.put(tid, 0, 10, new Object[] { "9527", str128, 2.0 }));
        KvIterator it = client.scan(tid, 0, "9527", "card", 12l, 9l);
        Assert.assertTrue(it != null);
        Assert.assertTrue(it.valid());
        Object[] row = it.getDecodedValue();
        Assert.assertTrue(row.length == 3);
        Assert.assertEquals(128, row[1].toString().length());
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals(2.0, row[2]);
        String str129 = new String(new byte[129]);
        try {
            client.put(tid, 0, 11, new Object[] { str129, "1221", 2.0 });
            Assert.assertFalse(true);
        } catch (Exception e) {
            Assert.assertFalse(false);
        }
        tabletClient.dropTable(tid, 0);
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
        Assert.assertTrue(client.put(tid, 0, 10l, new Object[] { "9527", null, 2.0d }));
        Assert.assertTrue(client.put(tid, 0, 1l, new Object[] { "9527", "test", null }));

        KvIterator it = client.scan(tid, 0, "9527", "card", 12l, 0l);
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
        Assert.assertTrue(client.put(tid, 0, 10l, new Object[] { "9527", "merchant0", 2.0d }));
        // check no exist
        try {
            Object[] row = client.getRow(tid, 0, "9528", 0l);
        } catch (TabletException e) {
            Assert.assertEquals(109, e.getCode());
        }

        // get head
        Object[]row = client.getRow(tid, 0, "9527", 0l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("merchant0", row[1]);
        Assert.assertEquals(2.0d, row[2]);
        // get by time
        row = client.getRow(tid, 0, "9527", 10l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("9527", row[0]);
        Assert.assertEquals("merchant0", row[1]);
        Assert.assertEquals(2.0d, row[2]);
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
        Assert.assertTrue(client.put(tid, 0, 10l, new Object[] { "a", "中文2", 2.0d }));
        Object[] row = client.getRow(tid, 0, "a", 0l);
        Assert.assertEquals(3, row.length);
        Assert.assertEquals("a", row[0]);
        Assert.assertEquals("中文2", row[1]);
        Assert.assertEquals(2.0d, row[2]);
    }
    
    @Test
    public void testNullDimension() {
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
        desc3.setType(ColumnType.kFloat);
        schema.add(desc3);
        boolean ok = tabletClient.createTable("schemaxxxx", tid, 0, 0, 8, schema);
        try {
            ok = client.put(tid, 0, 10, new Object[] { null, "1222", 1.0f });
            Assert.assertTrue(ok);
            KvIterator it = client.scan(tid, 0, "1222", "merchant", 12, 9);
            Assert.assertNotNull(it);
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(null, row[0]);
            Assert.assertEquals("1222", row[1]);
            Assert.assertEquals(1.0f, row[2]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        
        try {
            ok = client.put(tid, 0, 10, new Object[] { "9527", null, 1.0f });
            Assert.assertTrue(ok);
            KvIterator it = client.scan(tid, 0, "9527", "card", 12, 9);
            Assert.assertNotNull(it);
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals(null, row[1]);
            Assert.assertEquals(1.0f, row[2]);
        } catch (Exception e) {
            Assert.fail();
        }
        
        try {
            client.put(tid, 0, 10, new Object[] { null, null, 1.0f });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        
        try {
            client.put(tid, 0, 10, new Object[] { "", "", 1.0f });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        
    }
}
