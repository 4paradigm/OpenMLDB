/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.jdbc;

import com._4paradigm.openmldb.sdk.SdkOption;
import com._4paradigm.openmldb.sdk.SqlException;
import com._4paradigm.openmldb.sdk.SqlExecutor;
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class SQLDriver implements Driver {
    static {
        try {
            DriverManager.registerDriver(new SQLDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver", e);
        }
    }

    /**
     * Connect to the given connection string.
     *
     * @param url the url to connect to
     * @return a connection
     * @throws SQLException if it is not possible to connect
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException { 
        // Merge connectProperties (from URL) and supplied properties from user.
        // TODO(hw): only cluster mode now, support StandaloneOptions later
        if (info == null) {
            info = new Properties();
        }
        // just url missmatch, don't throw exception
        if (!parseAndMergeClusterProps(url, info)) {
            return null;
        }
        try {
            SdkOption option = createOptionByProps(info);
            SqlExecutor client = new SqlClusterExecutor(option);
            return new SQLConnection(client, info);
        } catch (SqlException e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * parse and verification of URL.
     *
     * <p>basic syntax :<br>
     * {@code
     * jdbc:openmldb://[<foobar>]/[<dbName>]?<zk>=<value1>&<zkPath>=<value2>[&<key>=<value>]
     * }
     * <p>'host:port' after '//' is useless.</p>
     * <p>zk:<br>
     * - simple :<br>
     * {@code <host>:<portnumber>}<br>
     * (for example localhost:6181)<br>
     * - list: <br>
     * {@code <host>:<portnumber>,<host>:<portnumber>,<host>:<portnumber>}<br>
     * <br>
     * <p>Some examples :<br>
     * {@code jdbc:openmldb:///?zk=localhost:6181&zkPath=/onebox}<br>
     * {@code
     * jdbc:openmldb:///db_test?zk=localhost:6181&zkPath=/onebox&sessionTimeout=1000&enableDebug=true}
     * <br>
     */
    private boolean parseAndMergeClusterProps(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return false;
        }
        parseInternal(url, info);
        return true;
    }

    /**
     * Parses the connection URL in order to set the UrlParser instance with all the information
     * provided through the URL.
     *
     * @param url        connection URL
     * @param properties properties
     * @throws SQLException if format is incorrect
     */
    private void parseInternal(String url, Properties properties) throws SQLException {
        try {
            // the first separator must be the end of header 'jdbc:openmldb://'
            int separator = url.indexOf("//");
            if (separator == -1) {
                throw new IllegalArgumentException(
                        "url parsing error: '//' is not present in the url " + url);
            }
            String urlSecondPart = url.substring(separator + 2);
            int dbIndex = urlSecondPart.indexOf("/");
            int paramIndex = urlSecondPart.indexOf("?");
            if (dbIndex < 0 || paramIndex < 0 || dbIndex > paramIndex) {
                throw new IllegalArgumentException("url parsing error: must have '/[<dbName>]?[params]' part, dbName can be empty");
            }

            String dbName = urlSecondPart.substring(dbIndex + 1, paramIndex);
            String additionalParameters = urlSecondPart.substring(paramIndex);
            // Connection may need the db name
            properties.setProperty("dbName", dbName);
            // set additional parameters to the properties
            parseParametersToProps(properties, additionalParameters);
        } catch (IllegalArgumentException e) {
            throw new SQLException("error parsing url: " + e.getMessage(), e);
        }
    }

    // If more, we should use option map <prop name, option name-type>.
    private SdkOption createOptionByProps(Properties properties) {
        SdkOption option = new SdkOption();
        // requires
        String prop = properties.getProperty("zk");
        if (prop != null) {
            option.setZkCluster(prop);
        } else {
            throw new IllegalArgumentException("must set param 'zk'");
        }
        prop = properties.getProperty("zkPath");
        if (prop != null) {
            option.setZkPath(prop);
        } else {
            throw new IllegalArgumentException("must set param 'zkPath'");
        }

        // optionals
        prop = properties.getProperty("sessionTimeout");
        if (prop != null) {
            option.setSessionTimeout(Long.parseLong(prop));
        }
        prop = properties.getProperty("enableDebug");
        if (prop != null) {
            option.setEnableDebug(Boolean.parseBoolean(prop));
        }
        prop = properties.getProperty("requestTimeout");
        if (prop != null) {
            option.setRequestTimeout(Long.parseLong(prop));
        }
        prop = properties.getProperty("zkLogLevel");
        if (prop != null) {
            option.setZkLogLevel(Integer.parseInt(prop));
        }
        prop = properties.getProperty("zkLogFile");
        if (prop != null) {
            option.setZkLogFile(prop);
        }
        prop = properties.getProperty("glogLevel");
        if (prop != null) {
            option.setGlogLevel(Integer.parseInt(prop));
        }
        prop = properties.getProperty("glogDir");
        if (prop != null) {
            option.setGlogDir(prop);
        }
        prop = properties.getProperty("maxSqlCacheSize");
        if (prop != null) {
            option.setMaxSqlCacheSize(Integer.parseInt(prop));
        }
        return option;
    }

    private void parseParametersToProps(Properties properties, String additionalParameters) {
        if (additionalParameters != null) {
            // params are after '?'
            String urlParameters = additionalParameters.substring(1);
            if (!urlParameters.isEmpty()) {
                String[] parameters = urlParameters.split("&");
                for (String parameter : parameters) {
                    int pos = parameter.indexOf('=');
                    if (pos == -1) {
                        properties.setProperty(parameter, "");
                    } else {
                        properties.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
                    }
                }
            }
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        // not support for "jdbc:openmldb:[<foobar>]//"
        return url != null && url.startsWith("jdbc:openmldb://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
