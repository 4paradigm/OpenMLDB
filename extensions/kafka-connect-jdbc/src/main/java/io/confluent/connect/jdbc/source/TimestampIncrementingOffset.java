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

package io.confluent.connect.jdbc.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TimestampIncrementingOffset {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
  static final String INCREMENTING_FIELD = "incrementing";
  static final String TIMESTAMP_FIELD = "timestamp";
  static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

  private final Long incrementingOffset;
  private final Timestamp timestampOffset;

  /**
   * @param timestampOffset the timestamp offset.
   *                        If null, {@link #getTimestampOffset()} will return
   *                        {@code new Timestamp(0)}.
   * @param incrementingOffset the incrementing offset.
   *                           If null, {@link #getIncrementingOffset()} will return -1.
   */
  public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset) {
    this.timestampOffset = timestampOffset;
    this.incrementingOffset = incrementingOffset;
  }

  public long getIncrementingOffset() {
    return incrementingOffset == null ? -1 : incrementingOffset;
  }

  public Timestamp getTimestampOffset() {
    return timestampOffset != null ? timestampOffset : new Timestamp(0L);
  }

  public boolean hasTimestampOffset() {
    return timestampOffset != null;
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>(3);
    if (incrementingOffset != null) {
      map.put(INCREMENTING_FIELD, incrementingOffset);
    }
    if (timestampOffset != null) {
      map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
      map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
    }
    return map;
  }

  public static TimestampIncrementingOffset fromMap(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
      return new TimestampIncrementingOffset(null, null);
    }

    Long incr = (Long) map.get(INCREMENTING_FIELD);
    Long millis = (Long) map.get(TIMESTAMP_FIELD);
    Timestamp ts = null;
    if (millis != null) {
      log.trace("millis is not null");
      ts = new Timestamp(millis);
      Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
      if (nanos != null) {
        log.trace("Nanos is not null");
        ts.setNanos(nanos.intValue());
      }
    }
    return new TimestampIncrementingOffset(ts, incr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimestampIncrementingOffset that = (TimestampIncrementingOffset) o;

    return Objects.equals(incrementingOffset, that.incrementingOffset)
        && Objects.equals(timestampOffset, that.timestampOffset);
  }

  @Override
  public int hashCode() {
    int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
    result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
    return result;
  }
}
