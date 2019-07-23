package com._4paradigm.rtidb.client.ut;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com._4paradigm.rtidb.client.base.TestCaseBase;
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

public class TabletAsyncClientTest extends TestCaseBase {

    private AtomicInteger id = new AtomicInteger(3000);
    private TabletAsyncClient client = null;
    private TabletSyncClient syncClient = null;

    @BeforeClass
    public void setUp() {
        super.setUp();
        client = super.tabletAsyncClient;
        syncClient = super.tabletSyncClient;

    }
    @AfterClass
    public void tearDown() {
        super.tearDown();
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
