package com._4paradigm.rtidb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.Table;

import io.brpc.client.RpcClient;



public class TabletSchemaClientTest {

    private final static AtomicInteger id = new AtomicInteger(1000);
    private static RpcClient rpcClient = null;
    private static TabletSyncClient client = null;
    static {
    	rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 9501, 100000, 3);
    	client = TabletClientBuilder.buildSyncClient(rpcClient);
    }
    
    @Test
    public void test0Create() {
    	int tid = id.incrementAndGet();
    	List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    	ColumnDesc desc1 = new ColumnDesc();
    	desc1.setAddTsIndex(true);
    	desc1.setIdx(0);
    	desc1.setName("card");
    	desc1.setType(ColumnType.kString);
    	schema.add(desc1);
    	ColumnDesc desc2 = new ColumnDesc();
    	desc2.setAddTsIndex(true);
    	desc2.setIdx(1);
    	desc2.setName("merchant");
    	desc2.setType(ColumnType.kString);
    	schema.add(desc2);
    	ColumnDesc desc3 = new ColumnDesc();
    	desc3.setAddTsIndex(false);
    	desc3.setIdx(2);
    	desc3.setName("amt");
    	desc3.setType(ColumnType.kDouble);
    	schema.add(desc3);
        boolean ok = client.createTable("tj0", tid, 0, 0, 8,schema);
        Assert.assertTrue(ok);
        
        Table table = client.getTable(tid, 0);
        
        Assert.assertTrue(table.getIndexes().size() == 2);
        Assert.assertTrue(table.getSchema().size() == 3);
        
        Assert.assertEquals(true , table.getSchema().get(0).isAddTsIndex());
        Assert.assertEquals("card" , table.getSchema().get(0).getName());
        Assert.assertEquals(ColumnType.kString , table.getSchema().get(0).getType());
        
        Assert.assertEquals(true , table.getSchema().get(1).isAddTsIndex());
        Assert.assertEquals("merchant" , table.getSchema().get(1).getName());
        Assert.assertEquals(ColumnType.kString , table.getSchema().get(1).getType());
        
        Assert.assertEquals(false , table.getSchema().get(2).isAddTsIndex());
        Assert.assertEquals("amt" , table.getSchema().get(2).getName());
        Assert.assertEquals(ColumnType.kDouble , table.getSchema().get(2).getType());
        Assert.assertEquals(table.getIndexes().get("card").intValue(), 0);
        Assert.assertEquals(table.getIndexes().get("merchant").intValue(), 1);
        client.dropTable(tid, 0);
    }
    
    @Test
    public void test1Put() throws TimeoutException, TabletException {
    	int tid = id.incrementAndGet();
    	List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    	ColumnDesc desc1 = new ColumnDesc();
    	desc1.setAddTsIndex(true);
    	desc1.setIdx(0);
    	desc1.setName("card");
    	desc1.setType(ColumnType.kString);
    	schema.add(desc1);
    	ColumnDesc desc2 = new ColumnDesc();
    	desc2.setAddTsIndex(true);
    	desc2.setIdx(1);
    	desc2.setName("merchant");
    	desc2.setType(ColumnType.kString);
    	schema.add(desc2);
    	ColumnDesc desc3 = new ColumnDesc();
    	desc3.setAddTsIndex(false);
    	desc3.setIdx(2);
    	desc3.setName("amt");
    	desc3.setType(ColumnType.kDouble);
    	schema.add(desc3);
        boolean ok = client.createTable("tj0", tid, 0, 0, 8,schema);
        Assert.assertTrue(ok);
        
        Assert.assertTrue(client.put(tid, 0, 10, new Object[] {"9527", "1222", 1.0}));
        client.dropTable(tid, 0);
    }
    
    @Test
    public void test2Scan() throws TimeoutException, TabletException {
    	int tid = id.incrementAndGet();
    	List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    	ColumnDesc desc1 = new ColumnDesc();
    	desc1.setAddTsIndex(true);
    	desc1.setIdx(0);
    	desc1.setName("card");
    	desc1.setType(ColumnType.kString);
    	schema.add(desc1);
    	ColumnDesc desc2 = new ColumnDesc();
    	desc2.setAddTsIndex(true);
    	desc2.setIdx(1);
    	desc2.setName("merchant");
    	desc2.setType(ColumnType.kString);
    	schema.add(desc2);
    	ColumnDesc desc3 = new ColumnDesc();
    	desc3.setAddTsIndex(false);
    	desc3.setIdx(2);
    	desc3.setName("amt");
    	desc3.setType(ColumnType.kDouble);
    	schema.add(desc3);
        boolean ok = client.createTable("tj0", tid, 0, 0, 8,schema);
        Assert.assertTrue(ok);
        Assert.assertTrue(client.put(tid, 0, 10, new Object[] {"9527", "1222", 1.0}));
        Assert.assertTrue(client.put(tid, 0, 11, new Object[] {"9527", "1221", 2.0}));
        Assert.assertTrue(client.put(tid, 0, 12, new Object[] {"9524", "1222", 3.0}));
        KvIterator it = client.scan(tid, 0, "9527", 0, 12l, 9);
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
        
        it = client.scan(tid, 0, "1222", 1, 12l, 9);
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
        
        client.dropTable(tid, 0);
    }

}
