package com._4paradigm.rtidb.client.ut;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;
import com.google.protobuf.ByteString;

import io.brpc.client.EndPoint;

public class TabletSyncClientTest {

    private AtomicInteger id = new AtomicInteger(6000);
    private static TabletSyncClientImpl client = null;
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
        client = new TabletSyncClientImpl(snc);
    }

    @AfterClass
    public static void tearDown() {
        snc.close();
    }
    @Test
    public void testInvalidTtlCreate() {
        int tid = id.incrementAndGet();
        boolean ok = client.createTable("tj0", tid, 0, -1, 8);
        Assert.assertFalse(ok);
    }

    @Test
    public void test0Create() {
        int tid = id.incrementAndGet();
        boolean ok = client.createTable("tj0", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = client.createTable("tj0", tid, 0, 0, 8);
        Assert.assertFalse(ok);
        client.dropTable(tid, 0);
    }

    @Test
    public void test1Put() throws TimeoutException {
        int tid = id.incrementAndGet();
        boolean ok = client.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = client.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(ok);
        ByteString buffer = client.get(tid, 0, "pk");
        Assert.assertNotNull(buffer);
        Assert.assertEquals("test0", buffer.toString(Charset.forName("utf-8")));
        client.dropTable(tid, 0);
    }

    @Test
    public void test3Scan() throws TimeoutException {
        int tid = id.incrementAndGet();
        boolean ok = client.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = client.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(ok);
        KvIterator it = client.scan(tid, 0, "pk", 9527l, 9526l);
        Assert.assertTrue(it != null);
        Assert.assertTrue(it.valid());
        Assert.assertEquals(9527l, it.getKey());
        ByteBuffer bb = it.getValue();
        Assert.assertEquals(5, bb.limit() - bb.position());
        byte[] buf = new byte[5];
        bb.get(buf);
        Assert.assertEquals("test0", new String(buf));
        it.next();
        client.dropTable(tid, 0);
    }

    @Test
    public void test4Drop() {
        int tid = id.incrementAndGet();
        boolean ok = client.dropTable(tid, 0);
        Assert.assertFalse(ok);
        ok = client.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = client.dropTable(tid, 0);
        Assert.assertTrue(ok);
    }
}
