package com._4paradigm.openmldb;

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import com._4paradigm.openmldb.server.impl.NLTabletServerImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NearLineTabletTest {
    private Random random;

    public NearLineTabletTest() {
        random = new Random(System.currentTimeMillis());
    }

    @Test
    void testCreateTable() {
        NLTabletServerImpl server = null;
        try {
            server = new NLTabletServerImpl();
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<Common.ColumnDesc> schema = new ArrayList<>();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("col1").setDataType(Type.DataType.kString).build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("col2").setDataType(Type.DataType.kBigInt).build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("col3").setDataType(Type.DataType.kTimestamp).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("col4").setDataType(Type.DataType.kDate).build();
        schema.add(col1);
        schema.add(col2);
        schema.add(col3);
        schema.add(col4);
        try {
            server.createTable("db_1", "table_" + random.nextInt(), "col1", schema);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            server.createTable("db_1", "table_" + random.nextInt(), "col2", schema);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            server.createTable("db_1", "table_" + random.nextInt(), "col3", schema);
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            server.createTable("db_1", "table_" + random.nextInt(), "col4", schema);
            Assert.assertTrue(true);
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
