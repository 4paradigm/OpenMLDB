package com._4paradigm.rtidb.client.functiontest.cases;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.DataProvider;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Listeners;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.client.schema.Table;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.TabletClientBuilder;
import com._4paradigm.rtidb.client.KvIterator;
import io.brpc.client.RpcClient;
import rtidb.api.Tablet.TTLType;


@Listeners({ com._4paradigm.rtidb.client.utils.TestReport.class })
public class TtlCreateTest {

  private final static AtomicInteger id = new AtomicInteger(1000);
  public static int tid = 0;
  private static RpcClient rpcClient = null;
  private static TabletSyncClient client = null;
  static {
    rpcClient = TabletClientBuilder.buildRpcClient("127.0.0.1", 37770, 100000, 3);
    client = TabletClientBuilder.buildSyncClient(rpcClient);
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

  @DataProvider(name = "latest")
  public Object[][] latest() {
    return new Object[][] {
        {0, true},
        {1, true},
        {-1, false},
        {1000, true},
    }; }

  @Test(dataProvider = "latest")
  public void testTtlCreateMD(long ttl, Boolean createOk) {
    Object[][] schemaArr = {
        {true, "card", ColumnType.kString},
        {false, "card1", ColumnType.kString},
        {false, "amt", ColumnType.kDouble}};
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    for (int i = 1; i < schemaArr.length; i++) {
      Object[] o = schemaArr[i];
      ColumnDesc desc = new ColumnDesc();
      Boolean index = (Boolean) o[0];
      desc.setAddTsIndex(index);
      desc.setName((String) o[1]);
      desc.setType((ColumnType) o[2]);
      schema.add(desc);
    }
    Boolean okMultiDimension = client.createTable("tj0", tid, 0, ttl, TTLType.kLatestTime, 8, schema);
    Assert.assertEquals(okMultiDimension, createOk);
  }

  @Test(dataProvider = "latest")
  public void testTtlCreateOne(long ttl, Boolean createOk) {
    Object[][] schemaArr = {
        {true, "card", ColumnType.kString},
        {false, "card1", ColumnType.kString},
        {false, "amt", ColumnType.kDouble}};
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    for (int i = 1; i < schemaArr.length; i++) {
      Object[] o = schemaArr[i];
      ColumnDesc desc = new ColumnDesc();
      Boolean index = (Boolean) o[0];
      desc.setAddTsIndex(index);
      desc.setName((String) o[1]);
      desc.setType((ColumnType) o[2]);
      schema.add(desc);
    }
    Boolean okOneDimension = client.createTable("tj0", tid, 0, ttl, TTLType.kLatestTime, 8);
    Assert.assertEquals(okOneDimension, createOk);
  }

  @DataProvider(name = "ttl")
  public Object[][] Ttl() {
    return new Object[][] {
        {true, 10000, 1, TTLType.kAbsoluteTime, "v1,10:0;v2,20:0;v3,30:0"},
        {true, 10001, 1, TTLType.kLatestTime, "v1,10:0;v2,20:0;v3,30:1"},
        {true, 10002, 1, TTLType.kLatestTime, "v1,20:0;v2,30:1;v3,10:0"},
        {true, 10003, 1, TTLType.kLatestTime, "v1,30:1;v2,10:0;v3,20:0"},
        {true, 10004, 1, TTLType.kLatestTime, "v1,10:0;v2,10:0;v3,30:1"},
        {true, 10005, 1, TTLType.kLatestTime, "v1,10:0;v2,30:1;v3,30:0"},
        {true, 10006, 1, TTLType.kLatestTime, "v1,10:0;v2,30:1"},
        {true, 10007, 2, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {true, 10008, 3, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {true, 10009, 1, TTLType.kLatestTime, "v1,10:1"},
        {true, 10010, 0, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {false, 10011, 1, TTLType.kAbsoluteTime, "v1,10:0;v2,20:0;v3,30:0"},
        {false, 10012, 1, TTLType.kLatestTime, "v1,10:0;v2,20:0;v3,30:1"},
        {false, 10013, 1, TTLType.kLatestTime, "v1,20:0;v2,30:1;v3,10:0"},
        {false, 10014, 1, TTLType.kLatestTime, "v1,30:1;v2,10:0;v3,20:0"},
        {false, 10015, 1, TTLType.kLatestTime, "v1,10:0;v2,10:0;v3,30:1"},
        {false, 10016, 1, TTLType.kLatestTime, "v1,10:0;v2,30:1;v3,30:0"},
        {false, 10017, 1, TTLType.kLatestTime, "v1,10:0;v2,30:1"},
        {false, 10018, 2, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {false, 10019, 3, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {false, 10020, 1, TTLType.kLatestTime, "v1,10:1"},
        {false, 10021, 0, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
    }; }

  @Test
  public void testReadyForTtlLatest() {
    Object[][] dataProvider = Ttl();

    for (Object[] dataProviderSingle: dataProvider) {
      boolean multiDimention = (Boolean) dataProviderSingle[0];
      int tid = (Integer) dataProviderSingle[1];
      int ttl = (Integer) dataProviderSingle[2];
      TTLType ttlType = (TTLType) dataProviderSingle[3];
      String ttlValues = String.valueOf(dataProviderSingle[4]);

      // 建表
      client.dropTable(tid, 0);
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
      Boolean ok = null;
      if (multiDimention) {
        ok = client.createTable("tj0", tid, 0, ttl, ttlType, 8, schema);
      } else {
        ok = client.createTable("tj0", tid, 0, ttl, ttlType, 8);
      }
      Assert.assertFalse(!ok);

      try {
        String[] data = ttlValues.split(";");
        // put数据
        for (String ele : data) {
          String key = ele.split(",")[0];
          Long ts = Long.valueOf(ele.split(",")[1].split(":")[0]);
          Boolean putok = null;
          if (multiDimention) {
            putok = client.put(tid, 0, ts, new Object[]{key, "value"});
          } else {
            putok = client.put(tid, 0, key, ts, "value");
          }
          Assert.assertFalse(!putok);
        }

        for (String ele : data) {
          String key = ele.split(",")[0];
          Boolean canScan = true ? ele.split(":")[1].equals("1") : false;
          KvIterator it = null;
          if (multiDimention) {
            it = client.scan(tid, 0, key, "card", 1999999999999L, 0);
          } else {
            it = client.scan(tid, 0, key, 1999999999999L, 0);
          }
          Assert.assertEquals(it.valid(), true);
          it.next();
        }
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail("data ready failed!");
      }
    }
    try {
      Thread.sleep(61000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test(dataProvider = "ttl", dependsOnMethods = { "testReadyForTtlLatest" })
  public void testTtlLatest(boolean multiDimention, int tid, long ttl, TTLType ttlType, String ttlValues) {
    try {
      String[] data = ttlValues.split(";");
      for (String ele : data) {
        String key = ele.split(",")[0];
        Boolean canScan = true ? ele.split(":")[1].equals("1") : false;
        KvIterator it = null;
        if (multiDimention) {
          it = client.scan(tid, 0, key, "card", 1999999999999L, 0);
        } else {
          it = client.scan(tid, 0, key, 1999999999999L, 0);
        }
        if (canScan) {
          Assert.assertEquals(it.valid(), true);
        } else {
          Assert.assertEquals(it.valid(), false);
        }
        it.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      client.dropTable(tid, 0);
    }
  }
}
