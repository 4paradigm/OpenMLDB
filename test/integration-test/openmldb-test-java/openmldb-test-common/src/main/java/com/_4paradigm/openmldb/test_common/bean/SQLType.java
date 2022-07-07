package com._4paradigm.openmldb.test_common.bean;

import com.google.common.collect.Sets;
import org.testng.collections.Lists;

import java.util.List;
import java.util.Set;

public enum SQLType {
    SELECT,
    DEPLOY,
    SHOW,
    // insert
    INSERT,
    CREATE,
    DROP,
    USE
    ;
    public static final Set<SQLType> RESULT_SET = Sets.newHashSet(SELECT, SHOW, DEPLOY);
//    public static final List<SQLType> VOID = Lists.newArrayList(CREATE,DROP,USE,INSERT);
    public static SQLType parseSQLType(String sql){
        if(sql.startsWith("select")){
            return SELECT;
        }else if (sql.startsWith("insert into")) {
            return INSERT;
        }else if (sql.startsWith("show")) {
            return SHOW;
        }else if (sql.startsWith("create")) {
            return CREATE;
        }else if (sql.startsWith("drop")) {
            return DROP;
        }else if (sql.startsWith("use")) {
            return USE;
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
