package com._4paradigm.fesql.common;
import com._4paradigm.fesql.type.TypeOuterClass;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class DDLEngine {


    public String genDDL(String sql, String schema) {
        String tempDB = "temp_" + System.currentTimeMillis();
//        try {
////            SQLEngine engine = new SQLEngine(sql, tempDB);
//        } catch (UnsupportedFesqlException e) {
//            e.printStackTrace();
//        }
        return "xx";
    }

    public static TypeOuterClass.TableDef getTableSchema(String jsonObject) {
        Gson gson = new Gson();
        JsonParser jsonParser = new JsonParser();
        JsonElement tableJson = jsonParser.parse(jsonObject);
        for (JsonElement e : tableJson.getAsJsonObject().get("tableInfo").getAsJsonArray()) {
            System.out.println(e);
        }

        TypeOuterClass.TableDef.Builder table = TypeOuterClass.TableDef.newBuilder();
        table.addColumns(TypeOuterClass.ColumnDef.newBuilder().setName("string").build());
        return table.build();

    }


    public static void main(String[] args) {
        String schemaPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/homecredit.json";
        String sqlPath = "/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-common/src/test/resources/ddl/homecredit.txt";
        File file = new File(schemaPath);
        try {
            getTableSchema(FileUtils.readFileToString(file, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
