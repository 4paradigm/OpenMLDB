package com._4paradigm.rtidb.client.functiontest.cases;

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

import io.brpc.client.EndPoint;


@Listeners({ com._4paradigm.rtidb.client.functiontest.utils.TestReport.class })
public class SPutTest {

  private final static AtomicInteger id = new AtomicInteger(100);
  private static int tid = 0;
  private static TabletSyncClient client = null;
  private static EndPoint endpoint = new EndPoint("127.0.0.1:37770");
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


  public static String genLongString(int len) {
    String str = "";
    for(int i = 0; i < len; i ++) {
      str += "a";
    }
    return str;
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
        {true, ColumnType.kString, "1111", true},
        {true, ColumnType.kString, " ", true},
        {true, ColumnType.kString, "、*&……%￥", true},
        {true, ColumnType.kString, "", false},
        {true, ColumnType.kString, null, false},
        {true, ColumnType.kString, genLongString(128), true},
        {true, ColumnType.kString, genLongString(129), false},
        {true, ColumnType.kFloat, 10.0f, true},
        {true, ColumnType.kFloat, 10.01f, true},
        {true, ColumnType.kFloat, -1e-1f, true},
        {true, ColumnType.kFloat, 1e-10f, true},
        {true, ColumnType.kFloat, null, false},
        {true, ColumnType.kInt32, 2147483647, true},
        {true, ColumnType.kInt32, 2147483648L, false},
        {true, ColumnType.kInt32, 1.1, false},
        {true, ColumnType.kInt32, 1e+5, false},
        {true, ColumnType.kInt32, "aaa", false},
        {true, ColumnType.kInt32, null, false},
        {true, ColumnType.kInt64, -9223372036854775808L, true},
        {true, ColumnType.kInt64, null, false},
        {true, ColumnType.kDouble, -1e-1d, true},
        {true, ColumnType.kDouble, -1e-10d, true},
        {true, ColumnType.kDouble, null, false},
        {true, ColumnType.kUInt32, 1, false},
        {true, ColumnType.kUInt32, null, false},
        {false, ColumnType.kString, "1111", true},
        {false, ColumnType.kString, " ", true},
        {false, ColumnType.kString, "、*&……%￥", true},
        {false, ColumnType.kString, "", true}, // scan value is null
        {false, ColumnType.kString, null, true},
        {false, ColumnType.kString, genLongString(128), true},
        {false, ColumnType.kString, genLongString(129), false},
        {false, ColumnType.kFloat, 10.0f, true},
        {false, ColumnType.kFloat, 10.01f, true},
        {false, ColumnType.kFloat, -1e-1f, true},
        {false, ColumnType.kFloat, 1e-10f, true},
        {false, ColumnType.kFloat, null, true},
        {false, ColumnType.kInt32, 2147483647, true},
        {false, ColumnType.kInt32, 2147483648L, false},
        {false, ColumnType.kInt32, 1.1, false},
        {false, ColumnType.kInt32, 1e+5, false},
        {false, ColumnType.kInt32, "aaa", false},
        {false, ColumnType.kInt32, null, true},
        {false, ColumnType.kInt64, -9223372036854775808L, true},
        {false, ColumnType.kInt64, null, true},
        {false, ColumnType.kDouble, -1e-1d, true},
        {false, ColumnType.kDouble, -1e-10d, true},
        {false, ColumnType.kDouble, null, true},
        {false, ColumnType.kUInt32, 1, false},
        {false, ColumnType.kUInt32, 0, false},
        {false, ColumnType.kUInt32, -1, false},
        {false, ColumnType.kUInt32, null, true},
    }; }

  @Test(dataProvider = "putdata")
  public void testPutColumnType(boolean isIndex, ColumnType type, Object value, boolean putOk) {
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    ColumnDesc desc1 = new ColumnDesc();
    desc1.setAddTsIndex(true);
    desc1.setName("card");
    desc1.setType(ColumnType.kString);
    schema.add(desc1);

    ColumnDesc desc2 = new ColumnDesc();
    if (isIndex) {
      desc2.setAddTsIndex(true);
    } else {
      desc2.setAddTsIndex(false);
    }
    desc2.setName("merchant");
    desc2.setType(type);
    schema.add(desc2);

    boolean ok = client.createTable("tj0", tid, 0, 144000, 8, schema);
    Assert.assertEquals(ok, true);

    Boolean putok = null;
    try {
        putok = client.put(tid, 0, System.currentTimeMillis(), new Object[]{"9527", value});
    } catch (Exception e) {
      putok = false;
      System.out.println("!!!!!" + e.getMessage());
    }
    Assert.assertFalse(!putok.equals(putOk));
    try {
      if (putOk) {
        KvIterator it = null;
        if (isIndex && System.currentTimeMillis() % 2 == 0) {
          it = client.scan(tid, 0, value.toString(), "merchant", 1999999999999L, 0);
        } else {
          it = client.scan(tid, 0, "9527", "card", 1999999999999L, 0);
        }
        Assert.assertFalse(it == null);

        Assert.assertFalse(!it.valid());
        Object[] row = it.getDecodedValue();
        Assert.assertTrue(row.length == 2);
        System.out.println(row[1]);
        Assert.assertEquals("9527", row[0]);
        if (type.equals(ColumnType.kNull) || value == "") {
          Assert.assertEquals(null, row[1]);
        } else {
          System.out.println(value);
          Assert.assertEquals(value, row[1]);
        }
        it.next();
      } else {
        Assert.assertFalse(putok);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }


  @DataProvider(name = "schema")
  public Object[][] schema() {
    return new Object[][] {
        new Object[][]{{true}, {"111", "222", "333"},
            {true, "card", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{true}, {"111", "222", "333"},
            {true, "card", ColumnType.kString},
            {true, "card1", ColumnType.kString},
            {true, "amt", ColumnType.kString}},
        new Object[][]{{true}, {"111", "222", "333"},
            {false, "card", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false}, {"111", "222"},
            {true, "card", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false}, {"111", "222", "333", "444"},
            {true, "card", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{true}, {"111"},
            {true, "amt", ColumnType.kString}},
        new Object[][]{{true}, {"111"},
            {false, "amt", ColumnType.kString}},
    }; }

  @Test(dataProvider = "schema")
  public void testPutIndex(Object[] ... array) {
    Boolean putOk = (Boolean) array[0][0];
    Object[] putData = array[1];
    List<ColumnDesc> schema = new ArrayList<ColumnDesc>();
    for (int i = 2; i < array.length; i++) {
      Object[] o = array[i];
      ColumnDesc desc = new ColumnDesc();
      desc.setAddTsIndex((Boolean) o[0]);
      desc.setName((String) o[1]);
      desc.setType((ColumnType) o[2]);
      schema.add(desc);
    }
    Boolean ok = client.createTable("tj0", tid, 0, 0, 8, schema);
    Assert.assertFalse(!ok);
    Boolean actPutOk = null;
    try {
      actPutOk = client.put(tid, 0, 10, putData);
    } catch (Exception e) {
      actPutOk = false;
      System.out.println("!!!!!" + e.getMessage());
    }
    Assert.assertEquals(actPutOk, putOk);
  }
}
