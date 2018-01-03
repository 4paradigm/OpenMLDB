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
import org.testng.annotations.Listeners;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.Table;

import io.brpc.client.RpcClient;

@Listeners({ com._4paradigm.rtidb.client.utils.TestReport.class })
public class SScanTest {

  private final static AtomicInteger id = new AtomicInteger(1000);
  private static int tid = 0;
  private static RpcClient rpcClient = null;
  private static TabletSyncClient client = null;
  static {
    rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 19521, 100000, 3);
    client = TabletClientBuilder.buildSyncClient(rpcClient);
  }

  @BeforeMethod
  public void setUp(){
    client.dropTable(tid, 0);
    tid = id.incrementAndGet();
    System.out.println("drop..." + tid);
  }

  @AfterMethod
  public void tearDown(){
    System.out.println("drop..." + tid);
    client.dropTable(tid, 0);
  }

  @DataProvider(name = "putdata")
  public Object[][] putdata() {
    return new Object[][] {
        {"card000", "card", true},
        {"card000", "merchant", false},
        {"merchant000", "merchant", false},
    }; }

  @Test(dataProvider = "putdata")
  public void testScan(String value, String schemaName, boolean scanOk) throws TimeoutException, TabletException {
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    ColumnDesc desc1 = new ColumnDesc();
    desc1.setAddTsIndex(true);
    desc1.setName("card");
    desc1.setType(ColumnType.kString);
    schema.add(desc1);

    ColumnDesc desc2 = new ColumnDesc();
    desc2.setAddTsIndex(false);
    desc2.setName("merchant");
    desc2.setType(ColumnType.kString);
    schema.add(desc2);

    Boolean ok = client.createTable("tj0", tid, 0, 0, 8, schema);
    Assert.assertEquals(ok, true);

    Assert.assertEquals(client.put(tid, 0, 30, new Object[] {"card000", "merchant333"}), true);
    Assert.assertEquals(client.put(tid, 0, 10, new Object[] {"card000", "merchant111"}), true);
    Assert.assertEquals(client.put(tid, 0, 20, new Object[] {"card000", "merchant222"}), true);

    KvIterator it = client.scan(tid, 0, value, schemaName, 1999999999999L, 0);
    Assert.assertEquals((it != null), scanOk);

    if (scanOk) {
      Assert.assertEquals(it.valid(), true);
      Object[] row = it.getDecodedValue();
      Assert.assertTrue(row.length == 2);
      Assert.assertEquals("card000", row[0]);
      Assert.assertEquals("merchant333", row[1]);
      it.next();
    }
  }
}
