package com._4paradigm.sql.jmh;

import java.sql.SQLException;

public interface DatabaseSetup {
    /**
     * Init connection to database & Setup data
     *  create new database & tables, generate records
     * @throws SQLException
     */
    void setup() throws SQLException;

    /**
     * Cleanup
     *  drop database & table, close database connection
     * @throws SQLException
     */
    void teardown() throws SQLException;
}
