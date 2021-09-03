package com._4paradigm.sql.jmh;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface QueryBenchmark {
    ResultSet query() throws SQLException;

    default String getTableName() {
        return "table_" + this.getClass().getSimpleName();
    }
}
