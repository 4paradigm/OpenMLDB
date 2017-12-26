package com._4paradigm.rtidb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.junit.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.Table;

import io.brpc.client.RpcClient;


public class SCreateTest {

  private final static AtomicInteger id = new AtomicInteger(1000);
  public static int tid = 0;
  private static RpcClient rpcClient = null;
  private static TabletSyncClient client = null;
  static {
    rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 19521, 100000, 3);
    client = TabletClientBuilder.buildSyncClient(rpcClient);
  }

  @BeforeMethod
  public void setUp(){
    tid = id.incrementAndGet();
  }

  @AfterMethod
  public void tearDown(){
    System.out.println("drop..." + tid);
    client.dropTable(tid, 0);
  }

  @DataProvider(name = "schema")
  public Object[][] Users() {
    return new Object[][] {
        new Object[][]{{true}, {true, 0, "card", ColumnType.kString}, {false, 0, "card1", ColumnType.kString}, {false, 2, "amt", ColumnType.kDouble}},
        new Object[][]{{true}, {true, 0, "card", ColumnType.kString}, {true, 1, "card1", ColumnType.kString}, {true, 2, "amt", ColumnType.kString}},
        new Object[][]{{true}, {false, 0, "card", ColumnType.kString}, {false, 1, "card1", ColumnType.kString}, {false, 2, "amt", ColumnType.kString}},
        new Object[][]{{false}, {false, 0, "card", ColumnType.kString}, {false, 1, "card", ColumnType.kString}},
        new Object[][]{{true}, {false, 0, "card", ColumnType.kString}},
    }; }


  @Test(dataProvider = "schema")
  public void test0Create(Object[] ... array) {
    Boolean result = (Boolean) array[0][0];
    System.out.println(tid);
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    int indexes = 0;
    int schemaCount = array.length - 1;
    for (int i = 1; i < array.length; i++) {
      Object[] o = array[i];
      ColumnDesc desc = new ColumnDesc();
      Boolean index = (Boolean) o[0];
      if (index) {
        indexes ++;
      }
      desc.setAddTsIndex(index);
      desc.setIdx((Integer) o[1]);
      desc.setName((String) o[2]);
      desc.setType((ColumnType) o[3]);
      schema.add(desc);
    }
    boolean ok = client.createTable("tj0", tid, 0, 0, 8, schema);
    System.out.println(ok);
    Assert.assertEquals(ok, result);
    if (ok) {
      Table table = client.getTable(tid, 0);
      Assert.assertEquals(table.getSchema().size(), schemaCount);
      Assert.assertEquals(table.getIndexes().size(), indexes);}
  }
}
