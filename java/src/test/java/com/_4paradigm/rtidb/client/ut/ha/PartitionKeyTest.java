package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionKeyTest extends TestCaseBase {
    private final static Logger logger = LoggerFactory.getLogger(ColumnKeyTest.class);
    private static AtomicInteger id = new AtomicInteger(50000);

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    @DataProvider(name = "StorageMode")
    public Object[][] StorageMode() {
        return new Object[][] {
                new Object[] { Common.StorageMode.kMemory },
                new Object[] { Common.StorageMode.kSSD },
                new Object[] { Common.StorageMode.kHDD },
        };
    }

    @Test
    public void testPartitionKey() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .addPartitionKey("mcc")
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            tableSyncClient.put(name, 1122, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            tableSyncClient.put(name, 1234, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0);
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }
}
