package com._4paradigm.openmldb.test_common.bean;

import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com.google.common.collect.Sets;
import org.testng.collections.Lists;

import java.util.List;
import java.util.Locale;
import java.util.Set;

public enum SQLType {
    SELECT,
    DEPLOY,
    SHOW,
    // insert
    INSERT,
    CREATE,
    DROP,
    USE,
    SET,
    DESC,
    JOB,
    OFFLINE_SELECT
    ;
    public static final Set<SQLType> RESULT_SET = Sets.newHashSet(SELECT, SHOW, DEPLOY);
//    public static final List<SQLType> VOID = Lists.newArrayList(CREATE,DROP,USE,INSERT);
    public static SQLType parseSQLType(String sql){
        if(sql.toLowerCase().startsWith("load data")||sql.toLowerCase().contains("into outfile")){
            return JOB;
        }else if(sql.toLowerCase().startsWith("select ")){
            if(OpenMLDBGlobalVar.EXECUTE_MODE.equals("offline")){
                return OFFLINE_SELECT;
            }
            return SELECT;
        }else if (sql.toLowerCase().startsWith("insert into ")) {
            return INSERT;
        }else if (sql.toLowerCase().startsWith("show ")) {
            return SHOW;
        }else if (sql.toLowerCase().startsWith("create ")) {
            return CREATE;
        }else if (sql.toLowerCase().startsWith("drop ")) {
            return DROP;
        }else if (sql.toLowerCase().startsWith("use ")) {
            return USE;
        }else if (sql.toLowerCase().startsWith("set ")) {
            return SET;
        }else if (sql.toLowerCase().startsWith("desc ")) {
            return DESC;
        }
        throw new IllegalArgumentException("no match sql type,sql:"+sql);
    }
    public static boolean isResultSet(SQLType sqlType){
        return RESULT_SET.contains(sqlType);
    }
    public static boolean isResultSet(String sql){
        return isResultSet(parseSQLType(sql));
    }
}
