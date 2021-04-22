package com._4paradigm.hybridse.sdk;

import com._4paradigm.hybridse.HybridSeLibrary;
import com._4paradigm.hybridse.vm.Engine;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import com._4paradigm.hybridse.type.TypeOuterClass;

public class RequestEngineTest {

    static {
        // Ensure native initialized
        HybridSeLibrary.initCore();
        Engine.InitializeGlobalLLVM();
    }

    @DataProvider(name = "requestEngineCase")
    public Object[] requestEngineCase() {
        return new Object[] {"select col1, col2, col3 col4, col5, col6 from t1;",
                "select col2+col3 as addcol23 from t1;",};
    }

    @Test(dataProvider = "requestEngineCase")
    public void RequestEngineTest(String sql) {
        TypeOuterClass.Database.Builder db = TypeOuterClass.Database.newBuilder();
        db.setName("db");

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
        try {
            RequestEngine engine = new RequestEngine(sql, db.build());
            Assert.assertNotNull(engine.getPlan());
        } catch (UnsupportedHybridSeException e) {
            e.printStackTrace();
            Assert.fail("fail to run request engine");
        }
    }
}
