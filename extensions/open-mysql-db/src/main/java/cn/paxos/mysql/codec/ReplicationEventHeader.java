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

public class ReplicationEventHeader {

  private long timestamp;
  private ReplicationEventType eventType;
  private long serverId;
  private long eventLength;
  // v3 (MySQL 4.0.2-4.1)
  private long nextPosition;
  private int flags; // TODO

  public ReplicationEventHeader(Builder builder) {
    timestamp = builder.timestamp;
    eventType = builder.eventType;
    serverId = builder.serverId;
    eventLength = builder.eventLength;
    nextPosition = builder.nextPosition;
    flags = builder.flags;
  }

  public static Builder builder() {
    return new Builder();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public ReplicationEventType getEventType() {
    return eventType;
  }

  public long getServerId() {
    return serverId;
  }

  public long getEventLength() {
    return eventLength;
  }

  public long getNextPosition() {
    return nextPosition;
  }

  public int getFlags() {
    return flags;
  }

  public static class Builder {
    private long timestamp;
    private ReplicationEventType eventType;
    private long serverId;
    private long eventLength;
    // v3 (MySQL 4.0.2-4.1)
    private long nextPosition;
    private int flags; // TODO

    public Builder timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public Builder eventType(ReplicationEventType eventType) {
      this.eventType = eventType;
      return this;
    }

    public Builder serverId(long serverId) {
      this.serverId = serverId;
      return this;
    }

    public Builder eventLength(long eventLength) {
      this.eventLength = eventLength;
      return this;
    }

    public Builder nextPosition(long nextPosition) {
      this.nextPosition = nextPosition;
      return this;
    }

    public Builder flags(int flags) {
      this.flags = flags;
      return this;
    }

    public ReplicationEventHeader build() {
      return new ReplicationEventHeader(this);
    }
  }
}
