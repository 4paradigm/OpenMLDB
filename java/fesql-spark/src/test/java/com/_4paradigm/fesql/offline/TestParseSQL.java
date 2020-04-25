package com._4paradigm.fesql.offline;

import com._4paradigm.fesql.base.BaseStatus;
import com._4paradigm.fesql.base.Slice;
import com._4paradigm.fesql.codec.Row;
import com._4paradigm.fesql.codec.RowBuilder;
import com._4paradigm.fesql.codec.RowView;
import com._4paradigm.fesql.type.TypeOuterClass;
import com._4paradigm.fesql.type.TypeOuterClass.TableDef;
import com._4paradigm.fesql.type.TypeOuterClass.ColumnDef;
import com._4paradigm.fesql.type.TypeOuterClass.Database;
import com._4paradigm.fesql.vm.*;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


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

        EngineOptions options = new EngineOptions();
        options.set_keep_ir(true);

        BaseStatus status = new BaseStatus();
        Engine engine = new Engine(catalog, options);
        assertTrue(engine.Get("select col_1, col_2 from t1;", "db", sess, status));
        CompileInfo compileInfo = sess.GetCompileInfo();

        long size = compileInfo.get_ir_size();
        ByteBuffer buffer = ByteBuffer.allocateDirect(Long.valueOf(size).intValue());
        compileInfo.get_ir_buffer(buffer);
        System.err.println("Dumped module string: len=" + size);

        PhysicalOpNode root = sess.GetPhysicalPlan();
        root.Print();

        FeSQLJITWrapper jit = new FeSQLJITWrapper();
        jit.Init();
        jit.AddModuleFromBuffer(buffer);

        dfs(jit, root);

        engine.delete();
        jit.delete();
        options.delete();
        status.delete();
        sess.delete();
        catalog.delete();
    }

    private void dfs(FeSQLJITWrapper jit, PhysicalOpNode root) {
        long childNum = root.GetProducerCnt();
        for (int k = 0; k < childNum; ++k) {
            PhysicalOpNode producer = root.GetProducer(k);
            dfs(jit, producer);
        }
        String name = root.GetFnName();
        if (!name.equals("")) {
            System.err.println(name + ": " + jit.FindFunction(name));
        }
        if (root.getType_() == PhysicalOpType.kPhysicalOpProject && root.GetProducer(0).GetProducerCnt() == 0) {
            System.err.println("Mock first project");
            run(jit, root);
        }
    }

    private void run(FeSQLJITWrapper jit, PhysicalOpNode root) {
        ByteBuffer rowBuf = ByteBuffer.allocateDirect(4096);

        RowBuilder builder = new RowBuilder(root.GetProducer(0).GetOutputSchema());
        builder.SetBuffer(rowBuf);
        builder.AppendDouble(3.14);
        builder.AppendInt32(42);

        Row row = new Row(rowBuf);
        long fn = jit.FindFunction(root.GetFnName());
        DummyRunner runner = new DummyRunner();
        Row output = runner.RunRowProject(fn, row);

        RowView rowView = new RowView(root.GetOutputSchema());
        rowView.Reset(output.buf(), Long.valueOf(output.size()).intValue());
        assertEquals(3.14, rowView.GetDoubleUnsafe(0), 1e-10);
        assertEquals(42, rowView.GetInt32Unsafe(1));

        output.delete();
        row.delete();
        runner.delete();
    }
}
