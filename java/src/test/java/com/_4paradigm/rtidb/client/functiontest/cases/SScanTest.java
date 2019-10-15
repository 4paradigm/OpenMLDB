package com._4paradigm.rtidb.client.functiontest.cases;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Listeners({com._4paradigm.rtidb.client.functiontest.utils.TestReport.class})
public class SScanTest extends TestCaseBase {

    private final static AtomicInteger id = new AtomicInteger(200);
    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }
    @DataProvider(name = "putdata")
    public Object[][] putdata() {
        return new Object[][]{
                {"card000", "card", true},
                {"card000", "merchant", false},
                {"merchant000", "merchant", false},
        };
    }

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
        int tid = id.incrementAndGet();
        tabletClient.dropTable(tid, 0);
        Boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertFalse(!ok);

        try {
            Long ctime = System.currentTimeMillis();
            Assert.assertEquals(tabletSyncClient.put(tid, 0, ctime - 10l, new Object[]{"card000", "merchant333"}), true);
            Assert.assertEquals(tabletSyncClient.put(tid, 0, ctime - 30l, new Object[]{"card000", "merchant111"}), true);
            Assert.assertEquals(tabletSyncClient.put(tid, 0, ctime - 20l, new Object[]{"card000", "merchant222"}), true);

            KvIterator it = tabletSyncClient.scan(tid, 0, value, schemaName, 1999999999999L, 0);
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
        tabletClient.dropTable(tid, 0);
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

        int tid = id.incrementAndGet();
        Boolean ok = tabletClient.createTable("tj0", tid, 0, 0, 8, schema);
        Assert.assertFalse(!ok);

        try {
            for (int i = 0; i < 10000; i++) {
                tabletSyncClient.put(tid, 0, 30, new Object[]{"merchant" + i, "card000"});
            }
            KvIterator it = tabletSyncClient.scan(tid, 0, "card000", "card", 1999999999999L, 0);
            Assert.assertFalse(it == null);
            int row = 0;
            while (it.valid()) {
                row++;
                it.next();
            }
            Assert.assertEquals(row, 10000);
        } catch (Exception e) {
            Assert.fail();
        }
        tabletClient.dropTable(tid, 0);
    }
}
