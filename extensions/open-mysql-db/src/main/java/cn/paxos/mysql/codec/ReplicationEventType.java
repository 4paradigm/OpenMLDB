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

/** https://mariadb.com/kb/en/2-binlog-event-header/ */
public enum ReplicationEventType {
  UNKNOWN_EVENT(0x00), /* mysql */
  START_EVENT_V3(0x01), /* mysql */
  QUERY_EVENT(0x02),
  STOP_EVENT(0x03),
  ROTATE_EVENT(0x04),
  INTVAR_EVENT(0x05), /* mysql */
  LOAD_EVENT(0x06), /* mysql */
  SLAVE_EVENT(0x07), /* mysql */
  CREATE_FILE_EVENT(0x08), /* mysql */
  APPEND_BLOCK_EVENT(0x09), /* mysql */
  EXEC_LOAD_EVENT(0x0a), /* mysql */
  DELETE_FILE_EVENT(0x0b), /* mysql */
  NEW_LOAD_EVENT(0x0c), /* mysql */
  RAND_EVENT(0x0d),
  USER_VAR_EVENT(0x0e),
  FORMAT_DESCRIPTION_EVENT(0x0f),
  XID_EVENT(0x10),
  BEGIN_LOAD_QUERY_EVENT(0x11), /* mysql */
  EXECUTE_LOAD_QUERY_EVENT(0x12), /* mysql */
  TABLE_MAP_EVENT(0x13),
  WRITE_ROWS_EVENTv0(0x14), /* mysql */
  UPDATE_ROWS_EVENTv0(0x15), /* mysql */
  DELETE_ROWS_EVENTv0(0x16), /* mysql */
  WRITE_ROWS_EVENTv1(0x17), /* mysql */
  UPDATE_ROWS_EVENTv1(0x18), /* mysql */
  DELETE_ROWS_EVENTv1(0x19), /* mysql */
  INCIDENT_EVENT(0x1a), /* mysql */
  HEARTBEAT_LOG_EVENT(0x1b),
  IGNORABLE_EVENT(0x1c), /* mysql */
  ROWS_QUERY_EVENT(0x1d), /* mysql */
  WRITE_ROWS_EVENTv2(0x1e), /* mysql */
  UPDATE_ROWS_EVENTv2(0x1f), /* mysql */
  DELETE_ROWS_EVENTv2(0x20), /* mysql */
  GTID_MYSQL_EVENT(0x21), /* mysql */
  ANONYMOUS_GTID_EVENT(0x22), /* mysql */
  PREVIOUS_GTID_EVENT(0x23), /* mysql */
  ANNOTATE_ROWS_EVENT(0xa0),
  BINLOG_CHECKPOINT_EVENT(0xa1),
  GTID_EVENT(0xa2),
  GTID_LIST_EVENT(0xa3),
  START_ENCRYPTION_EVENT(0xa4),
  FAKE_ROTATE_EVENT(0x04),
  FAKE_GTID_LIST_EVENT(0xa3);

  private final int value;

  ReplicationEventType(int value) {
    this.value = value;
  }

  public static ReplicationEventType lookup(int value) {
    for (ReplicationEventType columnType : values()) {
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
