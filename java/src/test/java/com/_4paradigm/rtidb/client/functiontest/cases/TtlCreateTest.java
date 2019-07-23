package com._4paradigm.rtidb.client.functiontest.cases;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ColumnType;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;
import com._4paradigm.rtidb.tablet.Tablet.TableStatus;
import com.google.protobuf.ByteString;
import org.testng.Assert;
import org.testng.annotations.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Listeners({com._4paradigm.rtidb.client.functiontest.utils.TestReport.class})
public class TtlCreateTest extends TestCaseBase {

    private final static AtomicInteger id = new AtomicInteger(400);

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }
    @DataProvider(name = "latest")
    public Object[][] latest() {
        return new Object[][]{{-1, false}, {0, true}, {1, true}, {1000, true}, {1001, false},};
    }

    @Test(dataProvider = "latest")
    public void testTtlCreateMD(long ttl, Boolean createOk) {
        Object[][] schemaArr = {{true, "card", ColumnType.kString}, {false, "card1", ColumnType.kString},
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
        int tid = id.incrementAndGet();
        Boolean okMultiDimension = tabletClient.createTable("tj0", tid, 0, ttl, TTLType.kLatestTime, 8, schema);
        Assert.assertEquals(okMultiDimension, createOk);
        tabletClient.dropTable(tid, 0);
    }

    @Test(dataProvider = "latest")
    public void testTtlCreateOne(long ttl, Boolean createOk) {
        Object[][] schemaArr = {{true, "card", ColumnType.kString}, {false, "card1", ColumnType.kString},
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
        int tid = id.incrementAndGet();
        Boolean okOneDimension = tabletClient.createTable("tj0", tid, 0, ttl, TTLType.kLatestTime, 8);
        Assert.assertEquals(okOneDimension, createOk);
        tabletClient.dropTable(tid, 0);
    }

    @DataProvider(name = "ttl")
    public Object[][] Ttl() {
        return new Object[][]{{true, id.incrementAndGet(), 1, TTLType.kAbsoluteTime, "v1,10:0;v2,20:0;v3,30:0"},
                {true, id.incrementAndGet() , 100, TTLType.kAbsoluteTime, "v1,10:0;v2," + System.currentTimeMillis() + ":1"},
                {true, id.incrementAndGet(), 0, TTLType.kAbsoluteTime, "v1,10:1;v2,30:1"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,20:0;v3,30:1"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,20:0;v2,30:1;v3,10:0"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,30:1;v2,10:0;v3,20:0"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,10:0;v3,30:1"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,30:0;v3,30:1"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,30:1"},
                {true, id.incrementAndGet(), 2, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
                {true, id.incrementAndGet(), 3, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
                {true, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:1"},
                {true, id.incrementAndGet(), 0, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
                {false, id.incrementAndGet(), 1, TTLType.kAbsoluteTime, "v1,10:0;v2,20:0;v3,30:0"},
                {false, id.incrementAndGet(), 100, TTLType.kAbsoluteTime, "v1,10:0;v2," + System.currentTimeMillis() + ":1"},
                {false, id.incrementAndGet(), 0, TTLType.kAbsoluteTime, "v1,10:1;v2,30:1"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,20:0;v3,30:1"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,20:0;v2,30:1;v3,10:0"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,30:1;v2,10:0;v3,20:0"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,10:0;v3,30:1"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,30:0;v3,30:1"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:0;v2,30:1"},
                {false, id.incrementAndGet(), 2, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
                {false, id.incrementAndGet(), 3, TTLType.kLatestTime, "v1,10:1;v2,30:1"},
                {false, id.incrementAndGet(), 1, TTLType.kLatestTime, "v1,10:1"},
                {false, id.incrementAndGet(), 0, TTLType.kLatestTime, "v1,10:1;v2,30:1"},};
    }

    private void prepareForTable(boolean multiDimention, int tid, long ttl, TTLType ttlType, String ttlValues) {
        // 建表
        tabletClient.dropTable(tid, 0);
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
        boolean ok = false;
        if (multiDimention) {
            ok = tabletClient.createTable("tj0", tid, 0, ttl, ttlType, 8, schema);
        } else {
            ok = tabletClient.createTable("tj0", tid, 0, ttl, ttlType, 8);
        }
        Assert.assertTrue(ok);
        System.out.println("create table " + tid + " ok");
        try {
            String[] data = ttlValues.split(";");
            // put数据
            for (String ele : data) {
                String merchant = ele.split(",")[0];
                Long ts = Long.valueOf(ele.split(",")[1].split(":")[0]);
                Boolean putok = null;
                if (multiDimention) {
                    putok = tabletSyncClient.put(tid, 0, ts, new Object[]{"pk", merchant});
                } else {
                    putok = tabletSyncClient.put(tid, 0, "pk", ts, merchant);
                }
                Assert.assertFalse(!putok);
            }
            TableStatus ts = tabletClient.getTableStatus(tid, 0);
            Assert.assertNotNull(ts);
            Assert.assertEquals(data.length, ts.getRecordCnt());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("data ready failed!");
            Assert.fail();
        }
    }


    @Test(dataProvider = "ttl")
    public void testTtl(boolean multiDimention, int tid, long ttl, TTLType ttlType, String ttlValues) {
        try {

            if (ttlType == TTLType.kLatestTime) {
                return;
            }
            prepareForTable(multiDimention, tid, ttl, ttlType, ttlValues);
            String[] data = ttlValues.split(";");
            KvIterator it = null;
            if (multiDimention) {
                it = tabletSyncClient.scan(tid, 0, "pk", "card", 1999999999999L, 0);
            } else {
                it = tabletSyncClient.scan(tid, 0, "pk", 1999999999999L, 0);
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
            while (it != null && it.valid()) {
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
            tabletClient.dropTable(tid, 0);
        }
    }

    @Test(dataProvider = "ttl")
    public void testLatest(boolean multiDimention, int tid, long ttl, TTLType ttlType, String ttlValues) {
        try {
            if (ttlType == TTLType.kAbsoluteTime) {
                return;
            }
            prepareForTable(multiDimention, tid, ttl, ttlType, ttlValues);
            String[] data = ttlValues.split(";");
            for (String ele : data) {
                Boolean canScan = true ? ele.split(":")[1].equals("1") : false;
                if (canScan) {
                    if (multiDimention) {
                        Object[] row = tabletSyncClient.getRow(tid, 0, "pk", Long.parseLong(ele.split(",")[1].split(":")[0]));
                        Assert.assertNotNull(row);
                        Assert.assertEquals(row[1], ele.split(",")[0]);
                    } else {
                        ByteString bs = tabletSyncClient.get(tid, 0, "pk", Long.parseLong(ele.split(",")[1].split(":")[0]));
                        Assert.assertNotNull(bs);
                        byte[] buf = new byte[2];
                        bs.asReadOnlyByteBuffer().get(buf);
                        String merchant = new String(buf);
                        Assert.assertEquals(merchant, ele.split(",")[0]);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tabletClient.dropTable(tid, 0);
        }
    }
}
