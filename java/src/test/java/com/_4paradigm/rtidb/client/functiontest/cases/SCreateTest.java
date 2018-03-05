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

import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;

import io.brpc.client.EndPoint;

@Listeners({ com._4paradigm.rtidb.client.functiontest.utils.TestReport.class })
public class SCreateTest {

  private final static AtomicInteger id = new AtomicInteger(10);
  public static int tid = 0;
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

  @DataProvider(name = "schema")
  public Object[][] Schema() {
    return new Object[][] {
        new Object[][]{{true},
            {true, "card", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kDouble}},
        new Object[][]{{true},
            {true, "card", ColumnType.kString},
            {true, "card1", ColumnType.kString},
            {true, "amt", ColumnType.kString}},
        new Object[][]{{true},
            {false, "card", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false},
            {true, "card", ColumnType.kString},
            {false, " ", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false},
            {true, " ", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false},
            {true, "card", ColumnType.kString},
            {false, "", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false},
            {true, "", ColumnType.kString},
            {false, "card1", ColumnType.kString},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{false},
            {false, "card", ColumnType.kDouble},
            {false, "card", ColumnType.kString}},
        new Object[][]{{true}, {false, "card", ColumnType.kString}},
        new Object[][]{{false}, {true, "", ColumnType.kString}},
        new Object[][]{{false}, {true, "   ", ColumnType.kString}},
        new Object[][]{{true}, {true, genLongString(128), ColumnType.kString}},
        new Object[][]{{true}, {false, genLongString(128), ColumnType.kString}},
        new Object[][]{{true},
            {true, genLongString(100), ColumnType.kString},
            {true, genLongString(29), ColumnType.kString}},
        new Object[][]{{false}, {true, genLongString(129), ColumnType.kString}},
        new Object[][]{{true},
            {true, "card", ColumnType.kFloat},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{true},
            {true, "card", ColumnType.kInt32},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{true},
            {true, "card", ColumnType.kInt64},
            {false, "amt", ColumnType.kString}},
        new Object[][]{{true},
            {true, "card", ColumnType.kUInt32},
            {false, "amt", ColumnType.kString}},
    }; }


  @Test(dataProvider = "schema")
  public void testCreate(Object[] ... array) {
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
      desc.setName((String) o[1]);
      desc.setType((ColumnType) o[2]);
      schema.add(desc);
    }
    Boolean ok = client.createTable("tj0", tid, 0, 0, 8, schema);
    System.out.println(ok);
    Assert.assertEquals(ok, result);
    if (ok) {
      TableHandler th = snc.getHandler(tid);
      Assert.assertEquals(th.getSchema().size(), schemaCount);
      Assert.assertEquals(th.getIndexes().size(), indexes);}
  }
}
