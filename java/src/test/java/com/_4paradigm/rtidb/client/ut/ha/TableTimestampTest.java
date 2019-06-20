package com._4paradigm.rtidb.client.ut.ha;

import java.util.concurrent.atomic.AtomicInteger;

import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.base.Config;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;

public class TableTimestampTest extends TestCaseBase {

    private static AtomicInteger id = new AtomicInteger(50000);
    private static String[] nodes = Config.NODES;

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public  void tearDown() {
        super.tearDown();
    }

    private String createSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("timestamp").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    @Test
    public void testTimestampPut() {
        String name = createSchemaTable();
        long time = System.currentTimeMillis();
        try {
            boolean ok = tableSyncClient.put(name, time, new Object[]{"card0", "mcc0", 1.1d, new DateTime(time)});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 0);
            Assert.assertNotNull(row);
            Assert.assertEquals(4, row.length);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc0", row[1]);
            Assert.assertEquals(1.1d, row[2]);
            Assert.assertEquals(time, ((DateTime) row[3]).getMillis());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }
}
