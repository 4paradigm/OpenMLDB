package com._4paradigm.rtidb.client.functiontest.cases;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
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
import com._4paradigm.rtidb.tablet.Tablet.TTLType;

import io.brpc.client.EndPoint;


@Listeners({com._4paradigm.rtidb.client.functiontest.utils.TestReport.class })
public class TtlCreateTest {

  private final static AtomicInteger id = new AtomicInteger(1000);
  public static int tid = 0;
  private static TabletSyncClient client = null;
  private static EndPoint endpoint = new EndPoint("192.168.33.10:9527");
  private static RTIDBClientConfig config = new RTIDBClientConfig();
  private static RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
  static {
      try {
          snc.init();
      } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      client = new TabletSyncClientImpl(snc);
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
        {-1, false},
        {0, true},
        {1, true},
        {1000, true},
        {1001, false},
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
        {true, 10001, 100, TTLType.kAbsoluteTime, "v1,10:0;v2," + System.currentTimeMillis() + ":1"},
        {true, 10002, 0, TTLType.kAbsoluteTime, "v1,10:1;v2,30:1"},
        {true, 10003, 1, TTLType.kLatestTime, "v1,10:0;v2,20:0;v3,30:1"},
        {true, 10004, 1, TTLType.kLatestTime, "v1,20:0;v2,30:1;v3,10:0"},
        {true, 10005, 1, TTLType.kLatestTime, "v1,30:1;v2,10:0;v3,20:0"},
        {true, 10006, 1, TTLType.kLatestTime, "v1,10:0;v2,10:0;v3,30:1"},
        {true, 10007, 1, TTLType.kLatestTime, "v1,10:0;v2,30:0;v3,30:1"},
        {true, 10008, 1, TTLType.kLatestTime, "v1,10:0;v2,30:1"},
        {true, 10009, 2, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {true, 10010, 3, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {true, 10011, 1, TTLType.kLatestTime, "v1,10:1"},
        {true, 10012, 0, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {false, 10020, 1, TTLType.kAbsoluteTime, "v1,10:0;v2,20:0;v3,30:0"},
        {false, 10021, 100, TTLType.kAbsoluteTime, "v1,10:0;v2," + System.currentTimeMillis() + ":1"},
        {false, 10022, 0, TTLType.kAbsoluteTime, "v1,10:1;v2,30:1"},
        {false, 10023, 1, TTLType.kLatestTime, "v1,10:0;v2,20:0;v3,30:1"},
        {false, 10024, 1, TTLType.kLatestTime, "v1,20:0;v2,30:1;v3,10:0"},
        {false, 10025, 1, TTLType.kLatestTime, "v1,30:1;v2,10:0;v3,20:0"},
        {false, 10026, 1, TTLType.kLatestTime, "v1,10:0;v2,10:0;v3,30:1"},
        {false, 10027, 1, TTLType.kLatestTime, "v1,10:0;v2,30:0;v3,30:1"},
        {false, 10028, 1, TTLType.kLatestTime, "v1,10:0;v2,30:1"},
        {false, 10029, 2, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {false, 10030, 3, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
        {false, 10031, 1, TTLType.kLatestTime, "v1,10:1"},
        {false, 10032, 0, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
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
          String merchant = ele.split(",")[0];
          Long ts = Long.valueOf(ele.split(",")[1].split(":")[0]);
          Boolean putok = null;
          if (multiDimention) {
            putok = client.put(tid, 0, ts, new Object[]{"pk", merchant});
          } else {
            putok = client.put(tid, 0, "pk", ts, merchant);
          }
          Assert.assertFalse(!putok);
        }
        KvIterator it = null;
        if (multiDimention) {
          it = client.scan(tid, 0, "pk", "card", 1999999999999L, 0);
        } else {
          it = client.scan(tid, 0, "pk", 1999999999999L, 0);
        }
        for (String ele : data) {
          Assert.assertEquals(it.valid(), true);
          it.next();
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.out.println("data ready failed!");
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
        KvIterator it = null;
        if (multiDimention) {
          it = client.scan(tid, 0, "pk", "card", 1999999999999L, 0);
        } else {
          it = client.scan(tid, 0, "pk", 1999999999999L, 0);
        }

        int scanRow = 0;
        List scanMerchantList = new ArrayList();
        for (String ele : data) {
          Boolean canScan = true ? ele.split(":")[1].equals("1") : false;
          if (canScan) {
            scanRow += 1;
            scanMerchantList.add(ele.split(",")[0]);
          }
        }
        List scanedMerchantList = new ArrayList();
        Object merchant = null;
        while(it.valid()) {
          if (multiDimention) {
            Object[] row = it.getDecodedValue();
            merchant = row[1];
          } else {
            ByteBuffer bb = it.getValue();
            byte[] buf = new byte[2];
            bb.get(buf);
            merchant = new String(buf);
          }
          scanedMerchantList.add(merchant);
          it.next();
        }
        System.out.println(scanMerchantList);
        System.out.println(scanedMerchantList);
        scanedMerchantList.removeAll(scanMerchantList);
        System.out.println(scanedMerchantList);
        Assert.assertEquals(scanedMerchantList, new ArrayList());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      client.dropTable(tid, 0);
    }
  }
}
