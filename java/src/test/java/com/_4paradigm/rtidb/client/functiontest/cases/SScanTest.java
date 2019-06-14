package com._4paradigm.rtidb.client.functiontest.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com._4paradigm.rtidb.client.ha.TableHandler;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;

import io.brpc.client.EndPoint;

@Listeners({ com._4paradigm.rtidb.client.functiontest.utils.TestReport.class })
public class SScanTest {

  private final static AtomicInteger id = new AtomicInteger(200);
  private static int tid = 0;
  private static TabletSyncClient client = null;
  private static EndPoint endpoint = new EndPoint("127.0.0.1:37770");
  private static RTIDBClientConfig config = new RTIDBClientConfig();
  private static RTIDBSingleNodeClient snc = null;
  static {
    config.setGlobalReadStrategies(TableHandler.ReadStrategy.kReadLeader);
    snc = new RTIDBSingleNodeClient(config, endpoint);
    try {
          snc.init();
      } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      client = new TabletSyncClientImpl(snc);
  }

  @AfterClass
  public void close() {
      snc.close();
  }
  @BeforeMethod
  public void setUp(){
    tid = id.incrementAndGet();
    System.out.println("drop..." + tid);
    client.dropTable(tid, 0);
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
  public void testScan(String value, String schemaName, boolean scanOk) {
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
    Assert.assertFalse(!ok);

    try {
      Long ctime = System.currentTimeMillis();
      Assert.assertEquals(client.put(tid, 0, ctime - 10l, new Object[] {"card000", "merchant333"}), true);
      Assert.assertEquals(client.put(tid, 0, ctime - 30l, new Object[] {"card000", "merchant111"}), true);
      Assert.assertEquals(client.put(tid, 0, ctime - 20l, new Object[] {"card000", "merchant222"}), true);

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
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testScanBigData() {
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    ColumnDesc desc1 = new ColumnDesc();
    desc1.setAddTsIndex(false);
    desc1.setName("merchant");
    desc1.setType(ColumnType.kString);
    schema.add(desc1);

    ColumnDesc desc2 = new ColumnDesc();
    desc2.setAddTsIndex(true);
    desc2.setName("card");
    desc2.setType(ColumnType.kString);
    schema.add(desc2);

    Boolean ok = client.createTable("tj0", tid, 0, 0, 8, schema);
    Assert.assertFalse(!ok);

    try {
      for (int i = 0; i < 10000; i ++) {
        client.put(tid, 0, 30, new Object[] {"merchant" + i, "card000"});
      }
      KvIterator it = client.scan(tid, 0, "card000", "card", 1999999999999L, 0);
      Assert.assertFalse(it == null);
      int row = 0;
      while (it.valid()){
        row ++;
        it.next();
      }
      Assert.assertEquals(row, 10000);
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
