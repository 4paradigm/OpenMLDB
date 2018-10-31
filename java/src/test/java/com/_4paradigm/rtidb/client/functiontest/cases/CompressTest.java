package com._4paradigm.rtidb.client.functiontest.cases;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.CompressType;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com._4paradigm.rtidb.utils.MurmurHash;
import com.google.protobuf.ByteString;
import io.brpc.client.EndPoint;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

@Listeners({ com._4paradigm.rtidb.client.functiontest.utils.TestReport.class })
public class CompressTest {

  private static String zkEndpoints = "127.0.0.1:2181";
  private static EndPoint endpoint = new EndPoint("127.0.0.1:37770");
  private static String leaderPath  = "/onebox/leader";
  private static RTIDBClientConfig config = new RTIDBClientConfig();
  private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
  private static RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
  private static RTIDBClusterClient ns_client = null;
  private static TabletSyncClient client = null;
  private static TableSyncClient tableSyncClient = null;
  private static TableAsyncClient tableAsyncClient = null;
  private static String[] nodes = new String[] {"127.0.0.1:9527", "127.0.0.1:9528", "127.0.0.1:9529"};
  private final static Logger logger = LoggerFactory.getLogger(CompressTest.class);
  static {
    try {
      nsc.init();
      snc.init();
      config.setZkEndpoints(zkEndpoints);
      config.setZkNodeRootPath("/onebox/nodes");
      config.setZkTableRootPath("/onebox/table/table_data");
      config.setZkTableNotifyPath("/onebox/table/notify");
      ns_client = new RTIDBClusterClient(config);
      ns_client.init();
      tableSyncClient = new TableSyncClientImpl(ns_client);
      tableAsyncClient = new TableAsyncClientImpl(ns_client);
      client = new TabletSyncClientImpl(snc);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static String genLongString(int len) {
    String str = "";
    for(int i = 0; i < len; i ++) {
      str += "a";
    }
    return str;
  }

  private String createKvTable(CompressType cType) {
    String name = String.valueOf(System.currentTimeMillis());
    PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
    PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
    TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
    TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();

    TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
        .setSegCnt(8).setName(name).setTtl(0).setCompressType(cType).build();

    boolean ok = nsc.createTable(table);
    ns_client.refreshRouteTable();
    Assert.assertTrue(ok);
    return name;
  }

  private String createSchemaTable(CompressType cType, Boolean isIndex1, String type1, Boolean isIndex2, String type2) {
    String name = String.valueOf(System.currentTimeMillis());
    PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
    PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
    PartitionMeta pm0_2 = PartitionMeta.newBuilder().setEndpoint(nodes[2]).setIsLeader(false).build();
    ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(isIndex1).setType(type1).build();
    ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(isIndex2).setType(type2).build();
    ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("string").build();
    TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
    TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
    TablePartition tp2 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_2).setPid(2).build();
    TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1).addTablePartition(tp2)
        .setSegCnt(8).setName(name).setTtl(0)
        .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
            .setCompressType(cType)
        .build();
    boolean ok = nsc.createTable(table);
    ns_client.refreshRouteTable();
    Assert.assertEquals(ok, true);
    return name;
  }


  @DataProvider(name = "putdataString")
  public Object[][] putdataString() {
    return new Object[][] {
        {"1111", true},
        {" ", true},
        {"、*&……%￥", true},
        {"", true},
        {genLongString(128), true},
        {genLongString(129), true},
    }; }

  @Test(dataProvider = "putdataString")
  public void testPutStringValue(String value, boolean putOk) {
    for (CompressType cType : CompressType.values()) {
      String tname = createKvTable(cType);
      try {
        boolean status = tableAsyncClient.put(tname, "test1", System.currentTimeMillis() + 9999, value).get();
        ByteString result = tableAsyncClient.get(tname, "test1").get();
        Assert.assertTrue(status == putOk);
        Assert.assertEquals(new String(result.toByteArray()), value);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      } finally {
        nsc.dropTable(tname);
      }
    }
  }


  @DataProvider(name = "putdataByte")
  public Object[][] putdataByte() {
    return new Object[][] {
        {"1111".getBytes(), true},
        {"".getBytes(), true},
        {new byte[]{}, true},
        {new byte[]{(byte)32, (byte)-3}, true},
        {new byte[]{(byte)10.01f}, true},
        {new byte[]{(byte)-1e-1f}, true},
    }; }

  @Test(dataProvider = "putdataByte")
  public void testPutByteValue(byte[] value, boolean putOk) {
    for (CompressType cType : CompressType.values()) {
      String tname = createKvTable(cType);
      try {
        boolean status = tableSyncClient.put(tname, "test1", System.currentTimeMillis() + 9999, value);
        ByteString result = tableSyncClient.get(tname, "test1");
        Assert.assertTrue(status == putOk);
        Assert.assertEquals(result.toByteArray(), value);
      } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      } finally {
        nsc.dropTable(tname);
      }
    }
  }


  @DataProvider(name = "putdataSchema")
  public Object[][] putGetScanSchema() {
    return new Object[][] {
        {true, "string", "1111", true, "string", "3", true},
        {true, "string", " ", true, "string", "1111", true},
        {true, "string", "、*&……%￥测试", true, "string", "、*&……%￥测试", true},
        {true, "string", "1111", true, "string", "", true},
        {true, "string", "1111", true, "string", null, true},
        {true, "string", "", true, "string", "", false},
        {true, "string", "", false, "string", "1111", false},
        {true, "string", "1111", false, "string", "", true},
        {true, "string", genLongString(128), true, "string", "1111", true},
        {true, "string", genLongString(129), true, "string", "1111", false},
        {true, "string", "1111", true, "float", 10.0f, true},
        {true, "string", "1111", true, "float", 10.01f, true},
        {true, "string", "1111", true, "float", -1e-1f, true},
        {true, "string", "1111", true, "float", 1e-10f, true},
        {true, "string", "1111", true, "float", "aaa", false},
        {true, "string", "1111", true, "float", null, true},
        {true, "string", "", true, "float", null, false},
        {true, "float", null, false, "string", "1111", false},
        {true, "string", "1111", true, "int32", 2147483647, true},
        {true, "string", "1111", true, "int32", 2147483648L, false},
        {true, "string", "1111", true, "int32", 1.1, false},
        {true, "string", "1111", true, "int32", 1e+5, false},
        {true, "string", "1111", true, "int32", "aaa", false},
        {true, "string", "1111", true, "int32", null, true},
        {true, "string", "1111", false, "int32", 2147483647, true},
        {true, "string", "1111", false, "int32", 2147483648L, false},
        {true, "string", "1111", false, "int32", 1.1, false},
        {true, "string", "1111", false, "int32", 1e+5, false},
        {true, "string", "1111", false, "int32", "aaa", false},
        {true, "string", "1111", false, "int32", null, true},
        {true, "int32", null, false, "string", "1111", false},
        {true, "string", "1111", true, "int64", -9223372036854775808L, true},
        {true, "string", "1111", true, "int64", "aaa", false},
        {true, "string", "1111", true, "int64", null, true},
        {true, "string", "1111", false, "int64", null, true},
        {true, "int64", null, false, "string", "1111", false},
        {true, "string", null, true, "int64", null, false},
        {true, "string", "1111", true, "double", -1e-1d, true},
        {true, "string", "1111", true, "double", -1e-10d, true},
        {true, "string", "1111", true, "double", "aaa", false},
        {true, "double", null, false, "string", "1111", false},
        {true, "string", "", true, "double", null, false},
        {true, "string", "1111", true, "double", null, true},
        {true, "string", "1111", false, "double", null, true},
    }; }

  @Test(dataProvider = "putdataSchema")
  public void testSchemaPutValue(Boolean isIndex1, String type1, Object value1,
                                 Boolean isIndex2, String type2, Object value2, Boolean putOk) {
    for (CompressType cType: CompressType.values()){
      Boolean ok = null;
      Boolean okT1 = null;
      Boolean okT2 = null;
      String name = createSchemaTable(cType, isIndex1, type1, isIndex2, type2);
      try {
        logger.info("type1 = "+type1+";  type2 = "+ type2+";");
        logger.info("value1 = "+value1+";  value2 = "+ value2+";");
        ok = tableSyncClient.put(name, 1555555555555L, new Object[]{value1, value2, "value3"});
        okT1 = tableSyncClient.put(name, 1666666666666L, new Object[]{"t1", value2, "amt2"});
        okT2 = tableSyncClient.put(name, 1777777777777L, new Object[]{"t2", value2, "amt3"});
        Assert.assertTrue(ok);
        Assert.assertTrue(okT1);
        Assert.assertTrue(okT2);
        if (putOk == true) {
          Object[] row = tableSyncClient.getRow(name, value1.toString(), 1555555555555L);
          Assert.assertEquals(row[0], value1);
          if (value2 != null && value2.equals("")) {
            value2 = "";
          }
          Assert.assertEquals(row[2], "value3");

          KvIterator it = tableSyncClient.scan(name, value1.toString(), "card", 1999999999999L, 1L);
          Assert.assertTrue(it.valid());
          Assert.assertTrue(it.getCount() == 1);
          Object[] rowScan = it.getDecodedValue();
          Assert.assertEquals(rowScan[0], value1);
          Assert.assertEquals(rowScan[1], value2);
          Assert.assertEquals(rowScan[2], "value3");
          it.next();

          TableHandler tbHandler = ns_client.getHandler(name);
          int tid = tbHandler.getTableInfo().getTid();
          int pid1 = (int) (MurmurHash.hash64(value1.toString()) % tbHandler.getPartitions().length);
          System.out.println("pid1 = " + pid1);
          int pid1Col2 = -1;

          it = tableSyncClient.scan(tid, pid1, value1.toString(), "card", 1999999999999L, 1L);
          rowScan = it.getDecodedValue();
          Assert.assertEquals(rowScan[0], value1);
          if (isIndex2 == true && value2 == "3") {
            pid1Col2 = (int) (MurmurHash.hash64(value2.toString()) % tbHandler.getPartitions().length);
            System.out.println("pid1Col2 = " + pid1Col2);
            it = tableSyncClient.scan(tid, pid1Col2, value2.toString(), "mcc",1666666666665L, 1L);
            rowScan = it.getDecodedValue();
            Assert.assertEquals(rowScan[0], value1);
            Assert.assertEquals(rowScan[1], value2);
          }

          int pid2 = (int) (MurmurHash.hash64("t1") % tbHandler.getPartitions().length);
          System.out.println("pid2 = " + pid2);
          it = tableSyncClient.scan(tid, pid2, "t1", "card", 1999999999999L, 1L);
          rowScan = it.getDecodedValue();
          Assert.assertEquals(rowScan[0], "t1");

          int pid3 = (int) (MurmurHash.hash64("t2") % tbHandler.getPartitions().length) * -1;
          System.out.println("pid3 = " + pid3);
          it = tableSyncClient.scan(tid, pid3, "t2", "card", 1999999999999L, 1L);
          rowScan = it.getDecodedValue();
          Assert.assertEquals(rowScan[0], "t2");

          Assert.assertFalse(pid1 == pid2 && pid1 == pid3 && pid2 == pid3);
        }
      } catch (Exception e) {
        ok = false;
        e.printStackTrace();
      } finally {
        System.out.println(ok);
        nsc.dropTable(name);
        Assert.assertEquals(ok, putOk);
      }
    }
  }
}
