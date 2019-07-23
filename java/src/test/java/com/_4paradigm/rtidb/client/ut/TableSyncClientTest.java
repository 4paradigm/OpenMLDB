package com._4paradigm.rtidb.client.ut;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
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
import com._4paradigm.rtidb.client.impl.TabletClientImpl;
import com.google.protobuf.ByteString;


public class TableSyncClientTest extends TestCaseBase {
    private AtomicInteger id = new AtomicInteger(7000);
    private TableSyncClient tableClient = null;
    private TabletClientImpl tabletClient = null;

    @BeforeClass
    public void setUp() {
        super.setUp();
        tableClient = super.tableSingleNodeSyncClient;
        tabletClient =super.tabletClient;

    }
    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testInvalidTtlCreate() {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.createTable("tj0", tid, 0, -1, 8);
        Assert.assertFalse(ok);
    }

    @Test
    public void test0Create() {
        int tid = id.incrementAndGet();
        tabletClient.dropTable(tid, 0);
        boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = tabletClient.createTable("tj0", tid, 0, 0, 8);
        Assert.assertFalse(ok);
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void test1Put() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = tableClient.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(ok);
        ByteString buffer = tableClient.get(tid, 0, "pk");
        Assert.assertNotNull(buffer);
        Assert.assertEquals("test0", buffer.toString(Charset.forName("utf-8")));
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void test1GetNullForKeyNotFound() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = tableClient.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(ok);
        ByteString buffer = tableClient.get(tid, 0, "pk");
        Assert.assertNotNull(buffer);
        Assert.assertEquals("test0", buffer.toString(Charset.forName("utf-8")));
        buffer = tableClient.get(tid, 0, "pknotfound");
        Assert.assertNull(buffer);
        tabletClient.dropTable(tid, 0);
    }

    @Test
    public void test3Scan() throws TimeoutException, TabletException {
        int tid = id.incrementAndGet();
        try {
            tableClient.scan(tid, 0, "pk", 9527, 9526);
            Assert.assertTrue(false);
        }catch (TabletException e) {
            Assert.assertTrue(true);
        }
        
        boolean ok = tabletClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = tableClient.put(tid, 0, "pk", 9527, "test0");
        Assert.assertTrue(ok);
        KvIterator it = tableClient.scan(tid, 0, "pk", 9527l, 9526l);
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
    public void test4Drop() {
        int tid = id.incrementAndGet();
        boolean ok = tabletClient.dropTable(tid, 0);
        Assert.assertFalse(ok);
        ok = tabletClient.createTable("tj1", tid, 0, 0, 8);
        Assert.assertTrue(ok);
        ok = tabletClient.dropTable(tid, 0);
        Assert.assertTrue(ok);
    }
}
