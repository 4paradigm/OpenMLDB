package com._4paradigm.rtidb.client.ut;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TabletAsyncClient;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TabletAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;

import io.brpc.client.EndPoint;

public class TabletAsyncClientTest {

    private AtomicInteger id = new AtomicInteger(3000);
    private static TabletAsyncClient client = null;
    private static TabletSyncClient syncClient = null;
    private static EndPoint endpoint = new EndPoint("127.0.0.1:9501");
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
        client = new TabletAsyncClientImpl(snc);
        syncClient = new TabletSyncClientImpl(snc);

    }
    @AfterClass
    public static void tearDown() {
        snc.close();
    }

    @Test
    public void test1Put() throws TimeoutException, InterruptedException, ExecutionException {
        int tid = id.incrementAndGet();
        boolean ok = syncClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        PutFuture future = client.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(future.get());
        GetFuture gf = client.get(tid, 0, "pk");
        Assert.assertEquals("test0", gf.get().toStringUtf8());
        syncClient.dropTable(tid, 0);
    }

    @Test
    public void test3Scan() throws TimeoutException, InterruptedException, ExecutionException {
        int tid = id.incrementAndGet();
        boolean ok = syncClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        PutFuture pf = client.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(pf.get());
        ScanFuture sf = client.scan(tid, 0, "pk", 9527l, 9526l);
        KvIterator it = sf.get();
        Assert.assertTrue(it != null);
        Assert.assertTrue(it.valid());
        Assert.assertEquals(9527l, it.getKey());
        ByteBuffer bb = it.getValue();
        Assert.assertEquals(5, bb.limit() - bb.position());
        byte[] buf = new byte[5];
        bb.get(buf);
        Assert.assertEquals("test0", new String(buf));
        it.next();
        syncClient.dropTable(tid, 0);
    }

}
