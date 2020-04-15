package com._4paradigm.fesql.offline;

import com._4paradigm.fesql.base.BaseStatus;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.type.TypeOuterClass.TableDef;
import com._4paradigm.fesql.type.TypeOuterClass.ColumnDef;
import com._4paradigm.fesql.type.TypeOuterClass.Database;
import com._4paradigm.fesql.vm.BatchRunSession;
import com._4paradigm.fesql.vm.Engine;
import com._4paradigm.fesql.vm.SimpleCatalog;
import org.junit.Test;


public class TestParseSQL {

    @Test
    public void testParseSQL() {
        LibraryLoader.loadLibrary("fesql_jsdk");
        ColumnDef col1 = ColumnDef.newBuilder()
                .setName("col_1").setType(TypeOuterClass.Type.kDouble).build();
        ColumnDef col2 = ColumnDef.newBuilder()
                .setName("col_2").setType(TypeOuterClass.Type.kInt32).build();
        TableDef table = TableDef.newBuilder()
                .setName("t1")
                .addColumns(col1)
                .addColumns(col2)
                .build();
        Database db = Database.newBuilder()
                .setName("db")
                .addTables(table).build();

        Engine.InitializeGlobalLLVM();

        SimpleCatalog catalog = new SimpleCatalog();
        catalog.AddDatabase(db);

        BatchRunSession sess = new BatchRunSession();

        BaseStatus status = new BaseStatus();
        Engine engine = new Engine(catalog);
        engine.Get("select col_1, col_2 from t1;", "db", sess, status);

        status.delete();
        sess.delete();
        engine.delete();
        catalog.delete();
    }

}
