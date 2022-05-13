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

package io.confluent.connect.jdbc.dialect;

import java.util.Objects;

/**
 * Options for dropping objects from a database. To use, simply create a new instance and then
 * use the setter methods.
 */
public class DropOptions {

  private final boolean ifExists;
  private final boolean cascade;

  /**
   * Create a new instance with the default settings.
   */
  public DropOptions() {
    this(false, false);
  }

  protected DropOptions(
      boolean ifExists,
      boolean cascade
  ) {
    this.ifExists = ifExists;
    this.cascade = cascade;
  }

  /**
   * Get whether the 'IF EXISTS' clause should be used with the 'DROP' statement.
   *
   * @return true if the object should be dropped only if it already exists, or false otherwise
   */
  public boolean ifExists() {
    return ifExists;
  }

  /**
   * Get whether the 'DROP' statement should cascade to dependent objects.
   *
   * @return true if dependent objects should also be dropped, or false otherwise
   */
  public boolean cascade() {
    return cascade;
  }

  /**
   * Set whether the 'IF EXISTS' clause should be used with the 'DROP' statement.
   *
   * @param ifExists true if the object should be dropped only if it already exists
   * @return a new options object with the current state plus the new if-exists state; never null
   */
  public DropOptions setIfExists(boolean ifExists) {
    return new DropOptions(ifExists, cascade);
  }

  /**
   * Set whether the 'DROP' statement should cascade to dependent objects.
   *
   * @param cascade true if dependent objects should also be dropped, or false otherwise
   * @return a new options object with the current state plus the new cascade state; never null
   */
  public DropOptions setCascade(boolean cascade) {
    return new DropOptions(ifExists, cascade);
  }

  @Override
  public String toString() {
    return "DropOptions{ifExists=" + ifExists + ", cascade=" + cascade + "}";
  }

  @Override
  public int hashCode() {
    return Objects.hash(ifExists, cascade);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof DropOptions) {
      DropOptions that = (DropOptions) obj;
      return this.ifExists() == that.ifExists() && this.cascade() == that.cascade();
    }
    return false;
  }
}
