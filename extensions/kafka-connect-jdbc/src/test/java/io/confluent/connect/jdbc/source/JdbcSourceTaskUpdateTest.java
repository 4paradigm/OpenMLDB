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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import io.confluent.connect.jdbc.util.DateTimeUtils;

// Tests of polling that return data updates, i.e. verifies the different behaviors for getting
// incremental data updates from the database
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class JdbcSourceTaskUpdateTest extends JdbcSourceTaskTestBase {
  private static final Map<String, String> QUERY_SOURCE_PARTITION
      = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                 JdbcSourceConnectorConstants.QUERY_NAME_VALUE);

  private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(ZoneOffset.UTC);

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testBulkPeriodicLoad() throws Exception {
    EmbeddedDerby.ColumnName column = new EmbeddedDerby.ColumnName("id");
    db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);

    // Bulk periodic load is currently the default
    task.start(singleTableConfig());

    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.insert(SINGLE_TABLE_NAME, "id", 2);
    records = task.poll();
    Map<Integer, Integer> twoRecords = new HashMap<>();
    twoRecords.put(1, 1);
    twoRecords.put(2, 1);
    assertEquals(twoRecords, countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.delete(SINGLE_TABLE_NAME, new EmbeddedDerby.EqualsCondition(column, 1));
    records = task.poll();
    assertEquals(Collections.singletonMap(2, 1), countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);
  }

  @Test(expected = ConnectException.class)
  public void testIncrementingInvalidColumn() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );


    PowerMock.replayAll();

    // Incrementing column must be NOT NULL
    db.createTable(SINGLE_TABLE_NAME, "id", "INT");

    startTask(null, "id", null);

    PowerMock.verifyAll();
  }

  @Test(expected = ConnectException.class)
  public void testTimestampInvalidColumn() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Timestamp column must be NOT NULL
    db.createTable(SINGLE_TABLE_NAME, "modified", "TIMESTAMP");

    startTask("modified", null, null);

    PowerMock.verifyAll();
  }

  @Test
  public void testManualIncrementing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION, 
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);

    startTask(null, "id", null);
    verifyIncrementingFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 3);

    verifyPoll(2, "id", Arrays.asList(2, 3), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrement() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    String extraColumn = "col";
    // Need extra column to be able to insert anything, extra is ignored.
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);

    startTask(null, "", null); // auto-incrementing
    verifyIncrementingFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // Adding records should result in only those records during the next poll()
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    verifyPoll(2, "id", Arrays.asList(2, 3), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestamp() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT");
    db.insert(SINGLE_TABLE_NAME,
        "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
        "id", 1);

    startTask("modified", null, null);
    verifyTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // If there isn't enough resolution, this could miss some rows. In this case, we'll only see
    // IDs 3 & 4.
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4);

    verifyPoll(2, "id", Arrays.asList(3, 4), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testMultiColumnTimestamp() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();
    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP",
                   "created", "TIMESTAMP NOT NULL",
                   "id", "INT");
    db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);
    startTask("modified, created", null, null);
    verifyMultiTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(13L), UTC_TIME_ZONE),
            "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2);
    db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3);
    db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4);

    verifyPoll(3, "id", Arrays.asList(2, 3, 4), false, false, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampWithDelay() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT");

    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);

    startTask("modified", null, null, 4L, "UTC");
    verifyTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    Long currentTime = new Date().getTime();

    // Validate that we are seeing 2,3 but not 4,5 as they are getting delayed to the next round
    // Using "toString" and not UTC because Derby's current_timestamp is always local time (i.e. doesn't honor Calendar settings)
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime).toString(), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime+1L).toString(), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime+500L).toString(), "id", 4);
    db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime+501L).toString(), "id", 5);

    verifyPoll(2, "id", Arrays.asList(2, 3), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // make sure we get the rest
    Thread.sleep(500);
    verifyPoll(2, "id", Arrays.asList(4, 5), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }


  @Test
  public void testComputeInitialOffsetWithTime() throws Exception{
    expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
    );
    PowerMock.replayAll();
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");
    startTask("modified", null, null, 4L, TimeZone.getDefault().getID(), 100L);

    Map<String, Object> result = task.computeInitialOffset("table", null, TimeZone.getDefault());
    Map<String, Object> expect = new HashMap<String, Object>();
    expect.put(TimestampIncrementingOffset.TIMESTAMP_FIELD, 100L);
    assertEquals(expect , result);
    PowerMock.verifyAll();
  }

  @Test
  public void testComputeInitialOffsetWithNull() throws Exception{
    expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
    );
    PowerMock.replayAll();
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");
    startTask("modified", null, null, 4L, TimeZone.getDefault().getID(), null);

    Map<String, Object> result = task.computeInitialOffset("table", null, TimeZone.getDefault());
    Map<String, Object> expect = null;
    assertEquals(expect , result);
    PowerMock.verifyAll();
  }

  @Test
  public void testComputeInitialOffsetWithCurrent() throws Exception{
    expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
    );
    PowerMock.replayAll();
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");
    startTask("modified", null, null, 4L, TimeZone.getDefault().getID(), -1L);

    Map<String, Object> result = task.computeInitialOffset("table", null, TimeZone.getDefault());
    Map<String, Object> expect = new HashMap<String, Object>();
    assertTrue(result.containsKey(TimestampIncrementingOffset.TIMESTAMP_FIELD));
    long gapWithIn = 1000L;
    long gap =new Date().getTime() - Long.valueOf(result.get(TimestampIncrementingOffset.TIMESTAMP_FIELD).toString());
    assertTrue(gapWithIn > gap);
    PowerMock.verifyAll();
  }


  @Test
  public void testTimestampWithTimestampInitialCurrent() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");

    Long currentTime = new Date().getTime();
    // Derby DB start with default Timezone
    TimeZone tz = TimeZone.getDefault();

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), tz), "id", 1);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(currentTime+1000L), tz), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(currentTime+1001L), tz), "id", 3);

    startTask("modified", null, null, 4L, tz.getID(), -1L);

    // expect records those timestamp is newer than current time.
    verifyPoll(2, "id", Arrays.asList(2, 3), true,false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampWithTimestampInitialDefault() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE), "id", 1);

    startTask("modified", null, null, 4L, "UTC", 0L);

    verifyTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE), "id", 3);

    verifyPoll(2, "id", Arrays.asList(2, 3), true,false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampWithTimestampInitial() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");

    Long currentTime = new Date().getTime();

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE), "id", 1);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(100L), UTC_TIME_ZONE), "id", 2);
    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(110L), UTC_TIME_ZONE), "id", 3);

    startTask("modified", null, null, 4L, "UTC" , 100L);

    // except a record its timestamp is newer than 100L
    verifyPoll(1, "id", Arrays.asList(3), true,false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.insert(SINGLE_TABLE_NAME, "modified", DateTimeUtils.formatTimestamp(new Timestamp(120L), UTC_TIME_ZONE), "id", 4);

    // except a record its timestamp is newer than previous offset
    verifyPoll(1, "id", Arrays.asList(4), true,false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampAndIncrementing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME,
        "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
        "id", 1);

    startTask("modified", "id", null);
    verifyIncrementingAndTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    // Should be able to pick up id 3 because of ID despite same timestamp.
    // Note this is setup so we can reuse some validation code
    db.insert(SINGLE_TABLE_NAME, "modified",
            DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE), "id", 3);
    db.insert(SINGLE_TABLE_NAME, "modified",
            DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE), "id", 1);

    verifyPoll(2, "id", Arrays.asList(3, 1), true, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampInNonUTCTimezone() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    String timeZoneID = "America/Los_Angeles";
    TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
        "modified", "TIMESTAMP NOT NULL",
        "id", "INT NOT NULL");
    String modifiedTimestamp = DateTimeUtils.formatTimestamp(new Timestamp(10L), timeZone);
    db.insert(SINGLE_TABLE_NAME, "modified", modifiedTimestamp, "id", 1);

    startTask("modified", "id", null, 0L, timeZoneID);
    verifyIncrementingAndTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampInInvalidTimezone() throws Exception {
    String invalidTimeZoneID = "Europe/Invalid";
    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
        "modified", "TIMESTAMP NOT NULL",
        "id", "INT NOT NULL");

    try {
      startTask("modified", "id", null, 0L, invalidTimeZoneID);
      fail("A ConfigException should have been thrown");
    } catch (ConnectException e) {
      assertTrue(e.getCause() instanceof ConfigException);
      ConfigException configException = (ConfigException) e.getCause();
      assertThat(configException.getMessage(),
          equalTo(
              "Invalid value Europe/Invalid for configuration db.timezone: Invalid time zone identifier"));
    }
  }

  @Test
  public void testMultiColumnTimestampAndIncrementing() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION,
        SINGLE_TABLE_PARTITION)
    );

    PowerMock.replayAll();

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP",
                   "created", "TIMESTAMP NOT NULL",
                   "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME,
        "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
        "id", 1);

    startTask("modified, created", "id", null);
    verifyIncrementingAndMultiTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    db.insert(SINGLE_TABLE_NAME, "created",
            DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE), "id", 3);
    db.insert(SINGLE_TABLE_NAME,
        "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
        "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
        "id", 1);

    verifyPoll(2, "id", Arrays.asList(3, 1), false, true, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testManualIncrementingRestoreNoVersionOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    testManualIncrementingRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap())
    );
  }

  @Test
  public void testManualIncrementingRestoreVersionOneOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    testManualIncrementingRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
    );
  }

  @Test
  public void testManualIncrementingRestoreOffsetsWithMultipleProtocol() throws Exception {
    TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(null, 0L);
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
    offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
    offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
    //we want to always use the offset with the latest protocol found
    testManualIncrementingRestoreOffset(offsets);
  }

  private void testManualIncrementingRestoreOffset(
      Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {
    expectInitialize(
        Arrays.asList(SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
        offsets
    );

    PowerMock.replayAll();

    db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
    db.insert(SINGLE_TABLE_NAME, "id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 3);

    startTask(null, "id", null);

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(2, 3), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testAutoincrementRestoreNoVersionOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    testAutoincrementRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap())
    );
  }

  @Test
  public void testAutoincrementRestoreVersionOneOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    testAutoincrementRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
    );
  }

  @Test
  public void testAutoincrementRestoreOffsetsWithMultipleProtocol() throws Exception {
    TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(null, 0L);
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
    offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
    offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
    //we want to always use the offset with the latest protocol found
    testAutoincrementRestoreOffset(offsets);
  }

  private void testAutoincrementRestoreOffset(
      Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {

    expectInitialize(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
        offsets
    );

    PowerMock.replayAll();

    String extraColumn = "col";
    // Use BIGINT here to test LONG columns
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY",
                   extraColumn, "FLOAT");
    db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
    db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

    startTask(null, "", null); // autoincrementing

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(2L, 3L), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampRestoreNoVersionOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
    testTimestampRestoreOffset(Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap()));
  }

  @Test
  public void testTimestampRestoreVersionOneOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
    testTimestampRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
    );
  }

  @Test
  public void testTimestampRestoreOffsetsWithMultipleProtocol() throws Exception {
    TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(
        new Timestamp(8L),
        null
    );
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
    offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
    offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
    //we want to always use the offset with the latest protocol found
    testTimestampRestoreOffset(offsets);
  }

  private void testTimestampRestoreOffset(
      Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {
    expectInitialize(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
        offsets
    );

    PowerMock.replayAll();

    // Timestamp is managed manually here so we can verify handling of duplicate values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT");
    // id=2 will be ignored since it has the same timestamp as the initial offset.
    db.insert(SINGLE_TABLE_NAME,
        "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
        "id", 2);
    db.insert(SINGLE_TABLE_NAME,
        "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
        "id", 3);
    db.insert(SINGLE_TABLE_NAME,
        "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
        "id", 4);

    startTask("modified", null, null);

    // Effectively skips first poll
    verifyPoll(2, "id", Arrays.asList(3, 4), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }

  @Test
  public void testTimestampAndIncrementingRestoreNoVersionOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
    testTimestampAndIncrementingRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap())
    );
  }

  @Test
  public void testTimestampAndIncrementingRestoreVersionOneOffset() throws Exception {
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
    testTimestampAndIncrementingRestoreOffset(
        Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
    );
  }

  @Test
  public void testTimestampAndIncrementingRestoreOffsetsWithMultipleProtocol() throws Exception {
    TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(new Timestamp(10L), 2L);
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
    offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
    offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
    //we want to always use the offset with the latest protocol found
    testTimestampAndIncrementingRestoreOffset(offsets);
  }

  private void testTimestampAndIncrementingRestoreOffset(
      Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {
    expectInitialize(Arrays.asList(
        SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
        offsets
    );

    PowerMock.replayAll();

    // Timestamp is managed manually here so we can verify handling of duplicate values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT NOT NULL");
    // id=3 will be ignored since it has the same timestamp + id as the initial offset, rest
    // should be included, including id=1 which is an old ID with newer timestamp
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(9L), UTC_TIME_ZONE),
            "id", 2);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 3);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 4);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 5);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(13L), UTC_TIME_ZONE),
            "id", 1);

    startTask("modified", "id", null);

    verifyPoll(3, "id", Arrays.asList(4, 5, 1), true, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

    PowerMock.verifyAll();
  }


  @Test
  public void testCustomQueryBulk() throws Exception {
    db.createTable(JOIN_TABLE_NAME, "user_id", "INT", "name", "VARCHAR(64)");
    db.insert(JOIN_TABLE_NAME, "user_id", 1, "name", "Alice");
    db.insert(JOIN_TABLE_NAME, "user_id", 2, "name", "Bob");

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "id", "INT",
                   "user_id", "INT");
    db.insert(SINGLE_TABLE_NAME, "id", 1, "user_id", 1);

    startTask(null, null, "SELECT \"test\".\"id\", \"test\""
                          + ".\"user_id\", \"users\".\"name\" FROM \"test\" JOIN \"users\" "
                          + "ON (\"test\".\"user_id\" = \"users\".\"user_id\")");

    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    Map<Integer, Integer> recordUserIdCounts = new HashMap<>();
    recordUserIdCounts.put(1, 1);
    assertEquals(recordUserIdCounts, countIntValues(records, "id"));
    assertRecordsTopic(records, TOPIC_PREFIX);
    assertRecordsSourcePartition(records, QUERY_SOURCE_PARTITION);

    db.insert(SINGLE_TABLE_NAME, "id", 2, "user_id", 1);
    db.insert(SINGLE_TABLE_NAME, "id", 3, "user_id", 2);
    db.insert(SINGLE_TABLE_NAME, "id", 4, "user_id", 2);

    records = task.poll();
    assertEquals(4, records.size());
    recordUserIdCounts = new HashMap<>();
    recordUserIdCounts.put(1, 2);
    recordUserIdCounts.put(2, 2);
    assertEquals(recordUserIdCounts, countIntValues(records, "user_id"));
    assertRecordsTopic(records, TOPIC_PREFIX);
    assertRecordsSourcePartition(records, QUERY_SOURCE_PARTITION);
  }

  @Test
  public void testCustomQueryWithTimestamp() throws Exception {
    expectInitializeNoOffsets(Arrays.asList(JOIN_QUERY_PARTITION));

    PowerMock.replayAll();

    db.createTable(JOIN_TABLE_NAME, "user_id", "INT", "name", "VARCHAR(64)");
    db.insert(JOIN_TABLE_NAME, "user_id", 1, "name", "Alice");
    db.insert(JOIN_TABLE_NAME, "user_id", 2, "name", "Bob");

    // Manage these manually so we can verify the emitted values
    db.createTable(SINGLE_TABLE_NAME,
                   "modified", "TIMESTAMP NOT NULL",
                   "id", "INT",
                   "user_id", "INT");
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1,
            "user_id", 1);

    startTask("modified", null, "SELECT \"test\".\"modified\", \"test\".\"id\", \"test\""
                                + ".\"user_id\", \"users\".\"name\" FROM \"test\" JOIN \"users\" "
                                + "ON (\"test\".\"user_id\" = \"users\".\"user_id\")");

    verifyTimestampFirstPoll(TOPIC_PREFIX);

    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2,
            "user_id", 1);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3,
            "user_id", 2);
    db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4,
            "user_id", 2);

    verifyPoll(2, "id", Arrays.asList(3, 4), true, false, false, TOPIC_PREFIX);

    PowerMock.verifyAll();
  }

  @Test (expected = ConnectException.class)
  public void testTaskFailsIfNoQueryOrTablesConfigProvided() {
    initializeTask();
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "[]");
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "");
    task.start(props);
  }

  @Test (expected = ConnectException.class)
  public void testTaskFailsIfBothQueryAndTablesConfigProvided() {
    initializeTask();
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, "[dbo.table]");
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, "Select * from some table");
    task.start(props);
  }

  private void startTask(String timestampColumn, String incrementingColumn, String query) {
    startTask(timestampColumn, incrementingColumn, query, 0L, "UTC");
  }

  private void startTask(String timestampColumn, String incrementingColumn, String query, Long delay, String timeZone) {
    startTask(timestampColumn, incrementingColumn, query, delay, timeZone, null);
  }

  private void startTask(String timestampColumn, String incrementingColumn, String query, Long delay, String timeZone, Long timestampInitial) {
    String mode = null;
    if (timestampColumn != null && incrementingColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING;
    } else if (timestampColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP;
    } else if (incrementingColumn != null) {
      mode = JdbcSourceConnectorConfig.MODE_INCREMENTING;
    } else {
      mode = JdbcSourceConnectorConfig.MODE_BULK;
    }
    initializeTask();
    Map<String, String> taskConfig = singleTableConfig();
    taskConfig.put(JdbcSourceConnectorConfig.MODE_CONFIG, mode);
    if (query != null) {
      taskConfig.put(JdbcSourceTaskConfig.QUERY_CONFIG, query);
      taskConfig.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
    }
    if (timestampColumn != null) {
      taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumn);
    }
    if (incrementingColumn != null) {
      taskConfig.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumn);
    }
    taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, delay == null ? "0" : delay.toString());

    if (timestampInitial != null) {
      taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CONFIG, timestampInitial.toString());
    }

    if (timeZone != null) {
      taskConfig.put(JdbcSourceConnectorConfig.DB_TIMEZONE_CONFIG, timeZone);
    }
    task.start(taskConfig);
  }

  private void verifyIncrementingFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertEquals(Collections.singletonMap(1L, 1), countIntIncrementingOffsets(records, "id"));
    assertIncrementingOffsets(records);
    assertRecordsTopic(records, topic);
  }

  private List<SourceRecord> verifyMultiTimestampFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertEquals(Collections.singletonMap(10L, 1), countTimestampValues(records, "created"));
    assertMultiTimestampOffsets(records);
    assertRecordsTopic(records, topic);
    return records;
  }

  private List<SourceRecord> verifyTimestampFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = task.poll();
    assertEquals(1, records.size());
    assertEquals(Collections.singletonMap(1, 1), countIntValues(records, "id"));
    assertEquals(Collections.singletonMap(10L, 1), countTimestampValues(records, "modified"));
    assertTimestampOffsets(records);
    assertRecordsTopic(records, topic);
    return records;
  }

  private void verifyIncrementingAndTimestampFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = verifyTimestampFirstPoll(topic);
    assertIncrementingOffsets(records);
  }

  private void verifyIncrementingAndMultiTimestampFirstPoll(String topic) throws Exception {
    List<SourceRecord> records = verifyMultiTimestampFirstPoll(topic);
    assertIncrementingOffsets(records);
  }



  private <T> void verifyPoll(int numRecords, String valueField, List<T> values,
                              boolean timestampOffsets, boolean incrementingOffsets, boolean multiTimestampOffsets,
                              String topic)
      throws Exception {
    List<SourceRecord> records = task.poll();
    int count = 0;
    while(records == null && count++ < 5) {
      records = task.poll();
      Thread.sleep(500);
    }
    assertNotNull(records);
    assertEquals(numRecords, records.size());

    HashMap<T, Integer> valueCounts = new HashMap<>();
    for(T value : values) {
      valueCounts.put(value, 1);
    }
    assertEquals(valueCounts, countIntValues(records, valueField));

    if (timestampOffsets) {
      assertTimestampOffsets(records);
    }
    if (incrementingOffsets) {
      assertIncrementingOffsets(records);
    }
    if (multiTimestampOffsets) {
      assertMultiTimestampOffsets(records);
    }

    assertRecordsTopic(records, topic);
  }

  private enum Field {
    KEY,
    VALUE,
    TIMESTAMP_VALUE,
    INCREMENTING_OFFSET,
    TIMESTAMP_OFFSET
  }

  @SuppressWarnings("unchecked")
  private <T> Map<T, Integer> countInts(List<SourceRecord> records, Field field, String fieldName) {
    Map<T, Integer> result = new HashMap<>();
    for (SourceRecord record : records) {
      T extracted;
      switch (field) {
        case KEY:
          extracted = (T)record.key();
          break;
        case VALUE:
          extracted = (T)((Struct)record.value()).get(fieldName);
          break;
        case TIMESTAMP_VALUE: {
          java.util.Date rawTimestamp = (java.util.Date) ((Struct)record.value()).get(fieldName);
          extracted = (T) (Long) rawTimestamp.getTime();
          break;
        }
        case INCREMENTING_OFFSET: {
          TimestampIncrementingOffset offset = TimestampIncrementingOffset.fromMap(record.sourceOffset());
          extracted = (T) (Long) offset.getIncrementingOffset();
          break;
        }
        case TIMESTAMP_OFFSET: {
          TimestampIncrementingOffset offset = TimestampIncrementingOffset.fromMap(record.sourceOffset());
          Timestamp rawTimestamp = offset.getTimestampOffset();
          extracted = (T) (Long) rawTimestamp.getTime();
          break;
        }
        default:
          throw new RuntimeException("Invalid field");
      }
      Integer count = result.get(extracted);
      count = (count != null ? count : 0) + 1;
      result.put(extracted, count);
    }
    return result;
  }

  private Map<Integer, Integer> countIntValues(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.VALUE, fieldName);
  }

  private Map<Long, Integer> countTimestampValues(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.TIMESTAMP_VALUE, fieldName);
  }

  private Map<Long, Integer> countIntIncrementingOffsets(List<SourceRecord> records, String fieldName) {
    return countInts(records, Field.INCREMENTING_OFFSET, fieldName);
  }


  private void assertIncrementingOffsets(List<SourceRecord> records) {
    // Should use incrementing field as offsets
    for(SourceRecord record : records) {
      Object incrementing = ((Struct)record.value()).get("id");
      long incrementingValue = incrementing instanceof Integer ? (long)(Integer)incrementing
                                                               : (Long)incrementing;
      long offsetValue = TimestampIncrementingOffset.fromMap(record.sourceOffset()).getIncrementingOffset();
      assertEquals(incrementingValue, offsetValue);
    }
  }

  private void assertTimestampOffsets(List<SourceRecord> records) {
    // Should use timestamps as offsets
    for(SourceRecord record : records) {
      Timestamp timestampValue = (Timestamp) ((Struct)record.value()).get("modified");
      Timestamp offsetValue = TimestampIncrementingOffset.fromMap(record.sourceOffset()).getTimestampOffset();
      assertEquals(timestampValue, offsetValue);
    }
  }

  private void assertMultiTimestampOffsets(List<SourceRecord> records) {
    for(SourceRecord record : records) {
      Timestamp timestampValue = (Timestamp) ((Struct)record.value()).get("modified");
      if (timestampValue == null) {
        timestampValue = (Timestamp) ((Struct)record.value()).get("created");
      }
      Timestamp offsetValue = TimestampIncrementingOffset.fromMap(record.sourceOffset()).getTimestampOffset();
      assertEquals(timestampValue, offsetValue);
    }
  }

  private void assertRecordsTopic(List<SourceRecord> records, String topic) {
    for (SourceRecord record : records) {
      assertEquals(topic, record.topic());
    }
  }

  private void assertRecordsSourcePartition(List<SourceRecord> records,
                                            Map<String, String> partition) {
    for (SourceRecord record : records) {
      assertEquals(partition, record.sourcePartition());
    }
  }
}
