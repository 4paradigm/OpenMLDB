package com._4paradigm.rtidb.client.ut;

import java.beans.Transient;
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
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletClientImpl;
import com.google.protobuf.ByteString;

import io.brpc.client.EndPoint;

public class TableAsyncClientTest {

    private AtomicInteger id = new AtomicInteger(11000);
    private static TableAsyncClientImpl tableClient = null;
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
        tableClient = new TableAsyncClientImpl(snc);
        tabletClient = new TabletClientImpl(snc);

    }
    @AfterClass
    public static void tearDown() {
        snc.close();
    }

    @Test
    public void test1Put() throws TimeoutException, InterruptedException, ExecutionException, TabletException {
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
    public void test3Scan() throws TimeoutException, InterruptedException, ExecutionException, TabletException {
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
    
    @Test
    public void test1GetNullForKeyNotFound() throws TimeoutException, TabletException, InterruptedException, ExecutionException {
        int tid = id.incrementAndGet();
        String tableName = "tj1";
        boolean ok = tabletClient.createTable(tableName, tid, 0, 0, 8);
        Assert.assertTrue(ok);
        PutFuture pf = tableClient.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(pf.get());
        GetFuture gf = tableClient.get(tid, 0, "pk");
        while(!gf.isDone()) {
            // waiting for gf is done
        }
        ByteString response = gf.get();
        Assert.assertNotNull(response);
        gf = tableClient.get(tid, 0, "pksss");
        while(!gf.isDone()) {
            // waiting for gf is done
        }
        response = gf.get();
        Assert.assertNull(response);
        tabletClient.dropTable(tid, 0);
    }

}
