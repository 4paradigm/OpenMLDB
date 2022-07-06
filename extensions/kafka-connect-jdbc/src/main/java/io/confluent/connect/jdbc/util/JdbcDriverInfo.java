/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;

/**
 * A summary of the version information about a JDBC driver and the database.
 */
public class JdbcDriverInfo {

  private final int jdbcMajorVersion;
  private final int jdbcMinorVersion;
  private final String jdbcDriverName;
  private final String productName;
  private final String productVersion;

  /**
   * Create the driver information.
   *
   * @param jdbcMajorVersion the major version of the JDBC specification supported by the driver
   * @param jdbcMinorVersion the minor version of the JDBC specification supported by the driver
   * @param jdbcDriverName   the name of the JDBC driver
   * @param productName      the name of the database product
   * @param productVersion   the version of the database product
   */
  public JdbcDriverInfo(
      int jdbcMajorVersion,
      int jdbcMinorVersion,
      String jdbcDriverName,
      String productName,
      String productVersion
  ) {
    this.jdbcMajorVersion = jdbcMajorVersion;
    this.jdbcMinorVersion = jdbcMinorVersion;
    this.jdbcDriverName = jdbcDriverName;
    this.productName = productName;
    this.productVersion = productVersion;
  }

  /**
   * Get the major version of the JDBC specification supported by the driver.
   *
   * @return the major version number
   */
  public int jdbcMajorVersion() {
    return jdbcMajorVersion;
  }

  /**
   * Get the minor version of the JDBC specification supported by the driver.
   *
   * @return the minor version number
   */
  public int jdbcMinorVersion() {
    return jdbcMinorVersion;
  }

  /**
   * Get the name of the database product.
   *
   * @return the name of the database product
   */
  public String productName() {
    return productName;
  }

  /**
   * Get the version of the database product.
   *
   * @return the version of the database product
   */
  public String productVersion() {
    return productVersion;
  }

  /**
   * Get the name of the JDBC driver.
   *
   * @return the name of the JDBC driver
   */
  public String jdbcDriverName() {
    return jdbcDriverName;
  }

  /**
   * Determine if the JDBC driver supports at least the specified major and minor version of the
   * JDBC specifications. This can be used to determine whether or not to call JDBC methods.
   *
   * @param jdbcMajorVersion the required major version of the JDBC specification
   * @param jdbcMinorVersion the required minor version of the JDBC specification
   * @return true if the driver supports at least the specified version of the JDBC specification,
   *     or false if the driver supports an older version of the JDBC specification
   */
  public boolean jdbcVersionAtLeast(
      int jdbcMajorVersion,
      int jdbcMinorVersion
  ) {
    if (this.jdbcMajorVersion() > jdbcMajorVersion) {
      return true;
    }
    if (jdbcMajorVersion == jdbcMajorVersion() && jdbcMinorVersion() >= jdbcMinorVersion) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (productName() != null) {
      sb.append(productName()).append(' ');
    }
    if (productVersion() != null) {
      sb.append(productVersion()).append(' ');
    }
    if (jdbcDriverName() != null) {
      sb.append(" using ").append(jdbcDriverName()).append(' ');
    }
    sb.append(jdbcMajorVersion()).append('.').append(jdbcMinorVersion());
    return sb.toString();
  }
}
