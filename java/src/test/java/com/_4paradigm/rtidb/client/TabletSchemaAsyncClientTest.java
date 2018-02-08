package com._4paradigm.rtidb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;

import io.brpc.client.RpcClient;

public class TabletSchemaAsyncClientTest {

    private final static AtomicInteger id = new AtomicInteger(4000);
    private static RpcClient rpcClient = null;
    private static TabletAsyncClient aclient = null;
    private static TabletSyncClient sclient = null;
    static {
        rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 9501, 100000, 3);
        aclient = TabletClientBuilder.buildAsyncClient(rpcClient);
        sclient = TabletClientBuilder.buildSyncClient(rpcClient);
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
        boolean ok = sclient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertTrue(ok);
        return tid;
    }
    
    @Test
    public void testAsyncScanTableNotExist() throws TimeoutException, TabletException {
        ScanFuture sf = aclient.scan(0, 0, "pl", "test_idx_name", 1000l, 0l);
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
        ScanFuture sf = aclient.scan(tid, 0, "pl", "card11", 1000l, 0l);
        try {
            sf.get();
            Assert.assertTrue(false);
        }  catch (InterruptedException e) {
            Assert.assertTrue(false);
        } catch (ExecutionException e) {
            Assert.assertTrue(true);
        }
    }
    
    public void testAsyncScanTableDataNotExist() {
        int tid = createTable();
        ScanFuture sf = aclient.scan(tid, 0, "pl", "card", 1000l, 0l);
        try {
            KvIterator it = sf.get();
            Assert.assertEquals(0, it.getCount());
            Assert.assertFalse(it.valid());
        }  catch (InterruptedException e) {
            Assert.assertTrue(false);
        } catch (ExecutionException e) {
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testAsyncScanTable() throws TimeoutException, TabletException {
        int tid = createTable();
        Assert.assertTrue(sclient.put(tid, 0, 10, new Object[] { "9527", "1222", 1.0 }));
        Assert.assertTrue(sclient.put(tid, 0, 11, new Object[] { "9527", "1221", 2.0 }));
        Assert.assertTrue(sclient.put(tid, 0, 12, new Object[] { "9524", "1222", 3.0 }));
        ScanFuture sf = aclient.scan(tid, 0, "9527", "card", 12, 9);
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

}
