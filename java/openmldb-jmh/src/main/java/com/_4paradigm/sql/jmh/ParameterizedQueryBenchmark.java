package com._4paradigm.sql.jmh;

/**
 * provide a set of rules of one benchmark case
 */
public interface ParameterizedQueryBenchmark extends QueryBenchmark {
    String param1 = "pk-2-55";
    String query = "SELECT col1, col2, col3, col4, col5 FROM %s WHERE col1=?";

    String ddl = "create table %s (col1 varchar, col2 timestamp, " +
            "col3 float," +
            "col4 float," +
            "col5 varchar," +
            "primary key (col1));";

    String insertValues = "(col1, col2, col3, col4, col5) values ('%s', %d, 100.0, 200.0, 'hello world');";

    String cleanDDL = "drop table %s";

    default int getRecordSize() {
        return 10000;
    }

    default String getInsertStmt() {
        return String.format("insert into %s %s", getTableName(), insertValues);
    }

    /**
     * ddl string to create the new table, useful in {@link DatabaseSetup#setup}
     *  remember to String.format with a table name
     * @return ddl
     */
    default String getDDL() {
        return String.format(ddl, getTableName());
    }

    default String getQuery() {
        return String.format(query, getTableName());
    }

    default String getCleanDDL() {
        return String.format(cleanDDL, getTableName());
    }
}