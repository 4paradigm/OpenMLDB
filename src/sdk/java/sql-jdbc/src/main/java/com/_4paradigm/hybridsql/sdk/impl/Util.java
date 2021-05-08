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

package com._4paradigm.hybridsql.sdk.impl;

import java.sql.SQLException;
import java.sql.Types;

public class Util {
    public static String sqlTypeToString(int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.SMALLINT:
                return "int16";
            case Types.INTEGER:
                return "int32";
            case Types.BIGINT:
                return "int64";
            case Types.FLOAT:
                return "float";
            case Types.DOUBLE:
                return "double";
            case Types.BOOLEAN:
                return "bool";
            case Types.VARCHAR:
                return "string";
            case Types.TIMESTAMP:
                return "timestamp";
            case Types.DATE:
                return "date";
            default:
                throw new SQLException("unsupported type: " + sqlType);
        }
    }
}
