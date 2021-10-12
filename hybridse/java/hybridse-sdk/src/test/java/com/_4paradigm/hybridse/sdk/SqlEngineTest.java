package com._4paradigm.hybridse.sdk;

import com._4paradigm.hybridse.HybridSeLibrary;
import com._4paradigm.hybridse.vm.Engine;
import com._4paradigm.hybridse.vm.EngineOptions;
import com._4paradigm.hybridse.type.TypeOuterClass;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.util.Arrays;
import static com._4paradigm.hybridse.sdk.SqlEngine.createDefaultEngineOptions;


public class SqlEngineTest {

    static {
        // Ensure native initialized
        HybridSeLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    @DataProvider(name = "sqlEngineCase")
    public Object[] sqlEngineCase() {
        return new Object[] {"select col1, col2, col3 col4, col5, col6 from t1;",
                "select col2+col3 as addcol23 from t1;",};
    }

    public TypeOuterClass.Database createTestDatabase(String dbName) {
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName(dbName);

        TypeOuterClass.TableDef.Builder tbl = TypeOuterClass.TableDef.newBuilder();
        tbl.setName("t1")
                .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col1").setIsNotNull(true)
                        .setType(TypeOuterClass.Type.kInt32).build())
                .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col2").setIsNotNull(true)
                        .setType(TypeOuterClass.Type.kInt64).build())
                .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col3").setIsNotNull(true)
                        .setType(TypeOuterClass.Type.kFloat).build())
                .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col4").setIsNotNull(true)
                        .setType(TypeOuterClass.Type.kDouble).build())
                .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col5").setIsNotNull(true)
                        .setType(TypeOuterClass.Type.kTimestamp).build())
                .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col6").setIsNotNull(true)
                        .setType(TypeOuterClass.Type.kVarchar).build());
        db.addTables(tbl.build());

        return db.build();
    }

    @Test(dataProvider = "sqlEngineCase")
    public void sqlEngineTest(String sql) {
        TypeOuterClass.Database db = createTestDatabase("db");

        try {
            SqlEngine engine = new SqlEngine(sql, db);
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

    @Test(dataProvider = "sqlEngineCase")
    public void sqlEngineTest2(String sql) {
        TypeOuterClass.Database db1 = createTestDatabase("db1");
        TypeOuterClass.Database db2 = createTestDatabase("db2");

        try {
            SqlEngine engine = new SqlEngine(sql, Arrays.<TypeOuterClass.Database>asList(db1, db2), "db1");
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

    @Test(dataProvider = "sqlEngineCase")
    public void sqlEngineTest3(String sql) {
        TypeOuterClass.Database db = createTestDatabase("db");

        try {
            SqlEngine engine = new SqlEngine(sql, db, SqlEngine.createDefaultEngineOptions());
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

    @Test(dataProvider = "sqlEngineCase")
    public void sqlEngineTest4(String sql) {
        TypeOuterClass.Database db1 = createTestDatabase("db1");
        TypeOuterClass.Database db2 = createTestDatabase("db2");

        try {
            SqlEngine engine = new SqlEngine(sql, Arrays.<TypeOuterClass.Database>asList(db1, db2),
                    SqlEngine.createDefaultEngineOptions(), "db2");
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

    @DataProvider(name = "sqlWindowLastJoinCase")
    public Object[] sqlWindowLastJoinCase() {
        return new Object[] {"" +
                " SELECT sum(t1.col1) over w1 as sum_t1_col1, t2.str1 as t2_str1\n" +
                " FROM t1\n" +
                " last join t2 order by t2.col1\n" +
                " on t1.col1 = t2.col1 and t1.col2 = t2.col0\n" +
                " WINDOW w1 AS (\n" +
                "  PARTITION BY t1.col2 ORDER BY t1.col1\n" +
                "  ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW\n" +
                " ) limit 10;",};
    }

    @Test(dataProvider = "sqlWindowLastJoinCase")
    public void sqlWindowLastJoin(String sql) {
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName("db");

        {
            TypeOuterClass.TableDef.Builder tbl = TypeOuterClass.TableDef.newBuilder();
            tbl.setName("t1")
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col0").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kVarchar).build())
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col1").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kInt32).build())
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col2").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kInt32).build());
            db.addTables(tbl.build());
        }
        {
            TypeOuterClass.TableDef.Builder tbl = TypeOuterClass.TableDef.newBuilder();
            tbl.setName("t2")
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("str0").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kVarchar).build())
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("str1").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kVarchar).build())
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col0").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kInt32).build())
                    .addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("col1").setIsNotNull(true)
                            .setType(TypeOuterClass.Type.kInt32).build());
            db.addTables(tbl.build());
        }
        try {
            EngineOptions options = createDefaultEngineOptions();
            options.SetEnableBatchWindowParallelization(true);
            SqlEngine engine = new SqlEngine(sql, db.build(), options);
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

}
