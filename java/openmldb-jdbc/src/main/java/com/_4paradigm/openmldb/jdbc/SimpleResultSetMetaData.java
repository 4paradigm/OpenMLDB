package com._4paradigm.openmldb.jdbc;

import static com._4paradigm.openmldb.sdk.impl.Util.sqlTypeToString;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class SimpleResultSetMetaData implements ResultSetMetaData {
    private final List<String> columnNames;
    private final List<Integer> columnSqlTypes;

    public SimpleResultSetMetaData(List<String> columnNames, List<Integer> columnSqlTypes) throws SQLException {
        this.columnNames = columnNames;
        this.columnSqlTypes = columnSqlTypes;
        if (columnNames.size() != columnSqlTypes.size()) {
            throw new SQLException(String.format("column names size %s != types size %s", columnNames.size(),
                    columnSqlTypes.size()));
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return null;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columnSqlTypes.get(column - 1);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return sqlTypeToString(columnSqlTypes.get(column - 1));
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
