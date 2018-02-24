package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletClientImpl;

import io.brpc.client.EndPoint;

public class TableAsyncClientTest {

    private AtomicInteger id = new AtomicInteger(11000);
    private static TableAsyncClientImpl tableClient = null;
    private static TabletClientImpl tabletClient = null;
    private static EndPoint endpoint = new EndPoint("127.0.0.1:9501");
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
    static {
        try {
            snc.init();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        tableClient = new TableAsyncClientImpl(snc);
        tabletClient = new TabletClientImpl(snc);
    }

    @Test
    public void test1Put() throws TimeoutException, InterruptedException, ExecutionException {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        PutFuture future = tableClient.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(future.get());
        GetFuture gf = tableClient.get(tid, 0, "pk");
        Assert.assertEquals("test0", gf.get().toStringUtf8());
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void test3Scan() throws TimeoutException, InterruptedException, ExecutionException {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        PutFuture pf = tableClient.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(pf.get());
        ScanFuture sf = tableClient.scan(tid, 0, "pk", 9527l, 9526l);
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
        tabletClient.dropTable(tid, 0);
    }

}
