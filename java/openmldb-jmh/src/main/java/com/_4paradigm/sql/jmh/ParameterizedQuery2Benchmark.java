package com._4paradigm.sql.jmh;

/**
 * parameterized query benchmark where have two filter conditions
 */
public interface ParameterizedQuery2Benchmark extends QueryBenchmark {
    String ddl = "create table %s (col1 int, col2 int, " +
            "col3 timestamp," +
            "col4 float," +
            "col5 varchar," +
            "primary key (col1));";
    String insertValues = " values (%d, %d, %d, 200.0, 'hello world');";

    int param1 = 500;
    int param2 = 40;

    default String getDDL() {
        return String.format(ddl, getTableName());
    }

    /**
     * number of records pre-inserted in the table
     * @return
     */
    default int getRecordSize() {
        return 10000;
    }

    default String getInsertStmt() {
        return String.format("insert into %s %s", getTableName(), insertValues);
    }

    default String getQuery() {
        return String.format("select col1, col2, col3, col4, col5 from %s where col1 > ? and col2 = ?;", getTableName());
    }

    default String getCleanDDL() {
        return String.format("drop table %s", getTableName());
    }
}
