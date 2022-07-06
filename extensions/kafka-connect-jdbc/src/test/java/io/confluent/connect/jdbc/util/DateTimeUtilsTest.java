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

import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DateTimeUtilsTest {

  private TimeZone utcTimeZone = TimeZone.getTimeZone(ZoneOffset.UTC);

  @Test
  public void testTimestampToNanosLong() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    long nanos = DateTimeUtils.toEpochNanos(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToNanosLongNull() {
    Long nanos = DateTimeUtils.toEpochNanos(null);
    assertNull(nanos);
    Timestamp timestamp = DateTimeUtils.toTimestamp((Long) null);
    assertNull(timestamp);
  }

  @Test
  public void testTimestampToNanosString() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    String nanos = DateTimeUtils.toEpochNanosString(timestamp);
    assertEquals(timestamp, DateTimeUtils.toTimestamp(nanos));
  }

  @Test
  public void testTimestampToNanosStringNull() {
    String nanos = DateTimeUtils.toEpochNanosString(null);
    assertNull(nanos);
    Timestamp timestamp = DateTimeUtils.toTimestamp((String) null);
    assertNull(timestamp);
  }

  @Test
  public void testTimestampToIsoDateTime() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(141362049);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcTimeZone);
    assertEquals("141362049", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcTimeZone));
  }

  @Test
  public void testTimestampToIsoDateTimeNanosLeading0s() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(1);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcTimeZone);
    assertEquals("000000001", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcTimeZone));
  }

  @Test
  public void testTimestampToIsoDateTimeNanosTrailing0s() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(100);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcTimeZone);
    assertEquals("000000100", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcTimeZone));
  }

  @Test
  public void testTimestampToIsoDateTimeNanos0s() {
    Timestamp timestamp = Timestamp.from(Instant.now());
    timestamp.setNanos(0);
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(timestamp, utcTimeZone);
    assertEquals("000000000", isoDateTime.substring(isoDateTime.lastIndexOf('.') + 1));
    assertEquals(timestamp, DateTimeUtils.toTimestampFromIsoDateTime(isoDateTime, utcTimeZone));
  }

  @Test
  public void testTimestampToIsoDateTimeNull() {
    String isoDateTime = DateTimeUtils.toIsoDateTimeString(null, utcTimeZone);
    assertNull(isoDateTime);
    Timestamp timestamp = DateTimeUtils.toTimestampFromIsoDateTime(null, utcTimeZone);
    assertNull(timestamp);
  }
}
