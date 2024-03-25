/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql.codec;

/**
 * https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
 *
 * https://dev.mysql.com/doc/dev/mysql-server/latest/namespaceclassic__protocol_1_1field__type.html#a1a187706992c835b40e8f1cf44d7b1d5
 */
public enum ColumnType {
  MYSQL_TYPE_DECIMAL(0x00),
  MYSQL_TYPE_TINY(0x01),
  MYSQL_TYPE_SHORT(0x02),
  MYSQL_TYPE_LONG(0x03),
  MYSQL_TYPE_FLOAT(0x04),
  MYSQL_TYPE_DOUBLE(0x05),
  MYSQL_TYPE_NULL(0x06),
  MYSQL_TYPE_TIMESTAMP(0x07),
  MYSQL_TYPE_LONGLONG(0x08),
  MYSQL_TYPE_INT24(0x09),
  MYSQL_TYPE_DATE(0x0a),
  MYSQL_TYPE_TIME(0x0b),
  MYSQL_TYPE_DATETIME(0x0c),
  MYSQL_TYPE_YEAR(0x0d),
  MYSQL_TYPE_NEWDATE(0x0e),
  MYSQL_TYPE_VARCHAR(0x0f),
  MYSQL_TYPE_BIT(0x10),
  MYSQL_TYPE_TIMESTAMP2(0x11),
  MYSQL_TYPE_DATETIME2(0x12),
  MYSQL_TYPE_TIME2(0x13),
  MYSQL_TYPE_JSON(0xf5), // Only used with MySQL. MariaDB uses MYSQL_TYPE_STRING for JSON.
  MYSQL_TYPE_NEWDECIMAL(0xf6),
  MYSQL_TYPE_ENUM(0xf7),
  MYSQL_TYPE_SET(0xf8),
  MYSQL_TYPE_TINY_BLOB(0xf9),
  MYSQL_TYPE_MEDIUM_BLOB(0xfa),
  MYSQL_TYPE_LONG_BLOB(0xfb),
  MYSQL_TYPE_BLOB(0xfc),
  MYSQL_TYPE_VAR_STRING(0xfd),
  MYSQL_TYPE_STRING(0xfe),
  MYSQL_TYPE_GEOMETRY(0xff);

  private final int value;

  ColumnType(int value) {
    this.value = value;
  }

  public static ColumnType lookup(int value) {
    for (ColumnType columnType : values()) {
      if (columnType.value == value) {
        return columnType;
      }
    }
    return null;
  }

  public int getValue() {
    return value;
  }
}
