package com._4paradigm.rtidb.client.ut;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import io.brpc.client.EndPoint;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class TableSchemaAsyncClientHandleNullTest {

    private final static AtomicInteger id = new AtomicInteger(9100);
    private static TableAsyncClientImpl tableClient = null;
    private static TabletClientImpl tabletClient = null;
    private static EndPoint endpoint = new EndPoint("127.0.0.1:9501");
//    private static EndPoint endpoint = new EndPoint("192.168.22.152:9501");
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);

    @BeforeClass
    public static void setUp() {
        try {
            snc.init();
            snc.getConfig().setHandleNull(true);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        tableClient = new TableAsyncClientImpl(snc);
        tabletClient = new TabletClientImpl(snc);

    }

    @AfterClass
    public static void tearDown() {
        for (int i = id.get(); i >= 9100; i--) {
            tabletClient.dropTable(i, 0);
        }
        snc.close();
    }

    private int createTable() {
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
        return tid;
    }

    @Test
    public void testAsyncScanTableNotExist() throws TimeoutException, TabletException {
        ScanFuture sf = tableClient.scan(0, 0, "pl", "test_idx_name", 1000l, 0l);
        try {
            sf.get();
            Assert.assertTrue(false);
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        } catch (ExecutionException e) {
            Assert.assertTrue(true);
        }
    }


    @Test
    public void testAsyncScanTableIdxNotExist() throws TimeoutException, TabletException {
        int tid = createTable();
        ScanFuture sf = tableClient.scan(tid, 0, "pl", "card11", 1000l, 0l);
        try {
            sf.get();
            Assert.assertTrue(false);
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        } catch (ExecutionException e) {
            Assert.assertTrue(true);
        }
    }


    @Test
    public void testAsyncScanTable() throws TimeoutException, TabletException, InterruptedException, ExecutionException {
        int tid = createTable();
        PutFuture pf = tableClient.put(tid, 0, 10, new Object[]{"9527", "1222", 1.0});
        Assert.assertTrue(pf.get());
        pf = tableClient.put(tid, 0, 11, new Object[]{"9527", "1221", 2.0});
        Assert.assertTrue(pf.get());
        pf = tableClient.put(tid, 0, 12, new Object[]{"9524", "1222", 3.0});
        Assert.assertTrue(pf.get());
        ScanFuture sf = tableClient.scan(tid, 0, "9527", "card", 12, 9);

        try {
            KvIterator it = sf.get();
            Assert.assertEquals(2, it.getCount());
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals("1221", row[1]);
            Assert.assertEquals(2.0, row[2]);
            it.next();

            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals("1222", row[1]);
            Assert.assertEquals(1.0, row[2]);
            it.next();
            Assert.assertFalse(it.valid());
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        } catch (ExecutionException e) {
            Assert.assertTrue(false);
        }


    }


    @Test
    public void testNullEmptyKeyScan() {
        int tid = createTable();
        try {
            PutFuture pf = tableClient.put(tid, 0, 10, new Object[]{null, "1222", 1.0});
            Assert.assertTrue(pf.get());

            pf = tableClient.put(tid, 0, 10, new Object[]{null, "2222", 1.0});
            Assert.assertTrue(pf.get());

            pf = tableClient.put(tid, 0, 11, new Object[]{null, "3222", 1.0});
            Assert.assertTrue(pf.get());

            pf = tableClient.put(tid, 0, 12, new Object[]{null, "4222", 1.0});
            Assert.assertTrue(pf.get());

            pf = tableClient.put(tid, 0, 13, new Object[]{"", "5222", 1.0});
            Assert.assertTrue(pf.get());

            pf = tableClient.put(tid, 0, 14, new Object[]{"", "6222", 1.0});
            Assert.assertTrue(pf.get());

            pf = tableClient.put(tid, 0, 15, new Object[]{"card1", "7222", 1.0});
            Assert.assertTrue(pf.get());

            ScanFuture sf = tableClient.scan(tid, 0, null, "card", 20, 9);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 4);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(null, row[0]);
            Assert.assertEquals("4222", row[1]);
            Assert.assertEquals(1.0, row[2]);

            sf = tableClient.scan(tid, 0, "", "card", 20, 9);
            it = sf.get();
            Assert.assertEquals(it.getCount(), 2);
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("", row[0]);
            Assert.assertEquals("6222", row[1]);
            Assert.assertEquals(1.0, row[2]);

            sf = tableClient.scan(tid, 0, "card1", "card", 20, 9);
            it = sf.get();
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card1", row[0]);
            Assert.assertEquals("7222", row[1]);
            Assert.assertEquals(1.0, row[2]);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            PutFuture pf = tableClient.put(tid, 0, 10, new Object[]{"9527", null, 1.0});
            Assert.assertTrue(pf.get());
            ScanFuture sf = tableClient.scan(tid, 0, "9527", "card", 12, 9);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals(null, row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (Exception e) {
            Assert.fail();
        }

    }
}
