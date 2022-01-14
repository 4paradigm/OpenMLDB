package com._4paradigm.hybridse.sdk;

import com._4paradigm.hybridse.HybridSeLibrary;
import com._4paradigm.hybridse.vm.Engine;
import com._4paradigm.hybridse.vm.EngineOptions;
import com._4paradigm.hybridse.type.TypeOuterClass;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

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
            options.SetEnableWindowColumnPruning(true);
            SqlEngine engine = new SqlEngine(sql, db.build(), options);
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

    @DataProvider(name = "sqlLastJoinWithMultipleDBCase")
    public Object[][] sqlLastJoinWithMultipleDBCase() {
        return new Object[][] {
                new Object[]{
                        "db1",
                        " SELECT sum(t1.col1) over w1 as sum_t1_col1, db2.t2.str1 as t2_str1\n" +
                                " FROM t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on t1.col1 = db2.t2.col1 and t1.col2 = db2.t2.col0\n" +
                                " WINDOW w1 AS (\n" +
                                "  PARTITION BY t1.col2 ORDER BY t1.col1\n" +
                                "  ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW\n" +
                                " ) limit 10;"
                },
                // default database is empty string
                new Object[]{
                        "",
                        " SELECT sum(db1.t1.col1) over w1 as sum_t1_col1, db2.t2.str1 as t2_str1\n" +
                                " FROM db1.t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on db1.t1.col1 = db2.t2.col1 and db1.t1.col2 = db2.t2.col0\n" +
                                " WINDOW w1 AS (\n" +
                                "  PARTITION BY db1.t1.col2 ORDER BY db1.t1.col1\n" +
                                "  ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW\n" +
                                " ) limit 10;"
                },
                // default database is null string
                new Object[]{
                        null,
                        " SELECT sum(db1.t1.col1) over w1 as sum_t1_col1, db2.t2.str1 as t2_str1\n" +
                                " FROM db1.t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on db1.t1.col1 = db2.t2.col1 and db1.t1.col2 = db2.t2.col0\n" +
                                " WINDOW w1 AS (\n" +
                                "  PARTITION BY db1.t1.col2 ORDER BY db1.t1.col1\n" +
                                "  ROWS_RANGE BETWEEN 3 PRECEDING AND CURRENT ROW\n" +
                                " ) limit 10;"
                }
        };
    }
    @Test(dataProvider = "sqlLastJoinWithMultipleDBCase")
    public void sqlLastJoinWithMultipleDB(String defaultDbName, String sql) {
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName("db1");

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
        TypeOuterClass.Database.Builder db2 = TypeOuterClass.Database.newBuilder();
        db2.setName("db2");
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
            db2.addTables(tbl.build());
        }
        try {
            EngineOptions options = createDefaultEngineOptions();
            options.SetEnableBatchWindowParallelization(true);
            options.SetEnableWindowColumnPruning(true);
            SqlEngine engine = new SqlEngine(sql, Lists.newArrayList(db.build(), db2.build()),
                    options, defaultDbName);
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run sql engine");
        }
    }

    @DataProvider(name = "sqlMultipleDBErrorCase")
    public Object[][] sqlMultipleDBErrorCase() {
        return new Object[][] {
                new Object[]{
                        "db1",
                        // t2.str1 should be db2.t2.str1
                        "SELECT t2.str1 as t2_str1\n" +
                                " FROM t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on t1.col1 = db2.t2.col1 and t1.col2 = db2.t2.col0;\n",
                        "SQL parse error: Column Not found: .t2.str1"
                },
                new Object[]{
                        "db1",
                        // db1.t2.str1 should be db2.t2.str1
                        "SELECT db1.t2.str1 as t2_str1\n" +
                                " FROM t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on t1.col1 = db2.t2.col1 and t1.col2 = db2.t2.col0;\n",
                        "SQL parse error: Column Not found: db1.t2.str1"},
                new Object[]{
                        "db1",
                        // t1.col1 = t2.col1 should be t1.col1 = db2.t2.col1
                        "SELECT db2.t2.str1 as t2_str1\n" +
                                " FROM t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on t1.col1 = t2.col1 and t1.col2 = db2.t2.col0;\n",
                        "SQL parse error: Column Not found: .t2.col1"},
                new Object[]{
                        // default database is empty string
                        "",
                        // t1.col1 = db2.t2.col1 should be db1.t1.col1 = db2.t2.col1
                        "SELECT db2.t2.str1 as t2_str1\n" +
                                " FROM t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on t1.col1 = db2.t2.col1 and t1.col2 = db2.t2.col0;\n",
                        "SQL parse error: Fail to transform data provider op: table t1 not exists in database []"},
                new Object[]{
                        // default database is null string
                        null,
                        // t1.col1 = t2.col1 should be t1.col1 = db2.t2.col1
                        "SELECT db2.t2.str1 as t2_str1\n" +
                                " FROM t1\n" +
                                " last join db2.t2 order by db2.t2.col1\n" +
                                " on t1.col1 = db2.t2.col1 and t1.col2 = db2.t2.col0;\n",
                        "SQL parse error: Fail to transform data provider op: table t1 not exists in database []"}


        };
    }
    @Test(dataProvider = "sqlMultipleDBErrorCase")
    public void sqlMultipleDBErrorTest(String defaultDbName, String sql, String errorMsg) {
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName("db1");

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
        TypeOuterClass.Database.Builder db2 = TypeOuterClass.Database.newBuilder();
        db2.setName("db2");
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
            db2.addTables(tbl.build());
        }
        try {
            EngineOptions options = createDefaultEngineOptions();
            options.SetEnableBatchWindowParallelization(true);
            options.SetEnableWindowColumnPruning(true);
            SqlEngine engine = new SqlEngine(sql,
                    Lists.newArrayList(db.build(), db2.build()), options, defaultDbName);
            Assert.assertNull(engine.getPlan());
            Assert.fail("Expect getPlan fail");
        } catch (UnsupportedHybridSeException e) {
            Assert.assertTrue(e.getMessage().contains(errorMsg));
        }
    }
}
